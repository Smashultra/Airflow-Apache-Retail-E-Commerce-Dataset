"""Print the evidence tables behind the anomaly rule base as Markdown.

Reads the raw Online Retail CSV with pandas, removes exact duplicate rows
like ``pyspark_clean.prepare_datasets`` and reports tables E1-E8 used in
``docs/ANOMALY_DETECTION_VI.md``. Analysis only; nothing is written.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import numpy as np
import pandas as pd

from pyspark_clean import RFM_EXCLUDED_DESCRIPTIONS, RFM_EXCLUDED_STOCK_CODES

TEXT_COLUMNS = ("InvoiceNo", "StockCode", "Description", "CustomerID", "Country")
QUANTILES = (0.25, 0.5, 0.75, 0.95, 0.99)
MIN_CUSTOMER_STOCK_LINES = 5
MIN_CUSTOMER_LINES = 20
MIN_CUSTOMER_INVOICES = 5
MIN_CHANNEL_SAMPLES = 30


def _norm(values: pd.Series) -> pd.Series:
    normalized = values.astype("string").str.strip().str.upper()
    return normalized.mask(normalized == "")


def _select(conditions: Sequence[pd.Series], choices: Sequence[str], default: str) -> np.ndarray:
    masks = [np.asarray(pd.Series(c).fillna(False), dtype=bool) for c in conditions]
    return np.select(masks, list(choices), default=default)


def load_dedup(path: str) -> pd.DataFrame:
    frame = pd.read_csv(
        path, encoding="ISO-8859-1", dtype={name: "string" for name in TEXT_COLUMNS}
    )
    frame["InvoiceDate"] = pd.to_datetime(frame["InvoiceDate"], format="%m/%d/%Y %H:%M")
    return frame.drop_duplicates().reset_index(drop=True)


def _accounting(frame: pd.DataFrame) -> pd.Series:
    invoice, stock = _norm(frame["InvoiceNo"]), _norm(frame["StockCode"])
    description = (
        _norm(frame["Description"]) if "Description" in frame
        else pd.Series(pd.NA, index=frame.index, dtype="string")
    )
    return (
        invoice.str.startswith("A").fillna(False)
        | (stock == "B").fillna(False)
        | (description == "ADJUST BAD DEBT").fillna(False)
    )


def invoice_channels(frame: pd.DataFrame) -> pd.Series:
    """Channel per normalized InvoiceNo, following the plan's priority."""
    work = pd.DataFrame({
        "invoice": _norm(frame["InvoiceNo"]),
        "identified": _norm(frame["CustomerID"]).notna(),
        "dot": (_norm(frame["StockCode"]) == "DOT").fillna(False),
        "priced": (frame["UnitPrice"] > 0).fillna(False) & ~_accounting(frame),
    }).dropna(subset=["invoice"])
    flags = work.groupby("invoice")[["identified", "dot", "priced"]].any()
    channel = _select(
        [flags["identified"], flags["dot"], flags["priced"]],
        ["identified", "retail_web", "retail_other"],
        default="internal",
    )
    return pd.Series(channel, index=flags.index, name="channel")


def annotate(frame: pd.DataFrame) -> pd.DataFrame:
    """Add normalized keys, channel, baseline group and record type."""
    result = frame.copy()
    result["invoice"] = _norm(result["InvoiceNo"])
    result["stock"] = _norm(result["StockCode"])
    result["customer"] = _norm(result["CustomerID"])
    result["description"] = _norm(result["Description"]).str.replace(r"\s+", " ", regex=True)
    result["line_value"] = result["Quantity"] * result["UnitPrice"]
    result["channel"] = result["invoice"].map(invoice_channels(result)).fillna("unknown")
    result["baseline_group"] = result["channel"].map({
        "identified": "identified", "retail_web": "retail", "retail_other": "retail",
    })
    positive_lines = (
        result.assign(positive=result["UnitPrice"] > 0)
        .groupby("invoice")["positive"].sum()
    )
    paid_invoice = result["invoice"].map(positive_lines).fillna(0) > 0
    service = (
        result["stock"].isin(RFM_EXCLUDED_STOCK_CODES).fillna(False)
        | result["description"].isin(RFM_EXCLUDED_DESCRIPTIONS).fillna(False)
        | result["stock"].str.fullmatch(r"[A-Z ]+").fillna(False)
    )
    price, quantity = result["UnitPrice"], result["Quantity"]
    result["record_type"] = _select(
        [
            _accounting(result),
            result["invoice"].str.startswith("C").fillna(False),
            service,
            (price == 0) & result["customer"].isna() & ~paid_invoice,
            (quantity > 0) & (price > 0),
            (price == 0) & (quantity > 0),
        ],
        ["accounting_adjustment", "cancellation", "fee_service",
         "inventory_adjustment", "sale", "zero_price_line"],
        default="unclassified",
    )
    return result


def _markdown(table: pd.DataFrame) -> str:
    def cell(value: object) -> str:
        if isinstance(value, (float, np.floating)):
            if np.isnan(value):
                return "–"
            return f"{value:,.0f}" if float(value).is_integer() and abs(value) >= 1000 else f"{value:,.4g}" if abs(value) < 1000 else f"{value:,.2f}"
        if isinstance(value, (int, np.integer)):
            return f"{value:,}"
        return str(value)

    frame = table.reset_index()
    header = "| " + " | ".join(str(column) for column in frame.columns) + " |"
    divider = "|" + "|".join("---" for _ in frame.columns) + "|"
    rows = ["| " + " | ".join(cell(value) for value in row) + " |" for row in frame.itertuples(index=False)]
    return "\n".join([header, divider, *rows])


def _section(title: str, table: pd.DataFrame) -> None:
    print(f"\n### {title}\n")
    print(_markdown(table))


def e1_dot(data: pd.DataFrame) -> pd.DataFrame:
    dot = data[data["stock"] == "DOT"].assign(has_id=lambda f: f["customer"].notna())
    return dot.groupby("has_id").agg(
        invoices=("invoice", "nunique"), lines=("invoice", "size"),
        value_gbp=("line_value", "sum"), median_price=("UnitPrice", "median"),
    )


def e2_channels(data: pd.DataFrame) -> pd.DataFrame:
    positive = data["line_value"].where((data["Quantity"] > 0) & (data["UnitPrice"] > 0), 0)
    table = data.assign(positive_value=positive).groupby("channel").agg(
        invoices=("invoice", "nunique"), lines=("invoice", "size"),
        positive_value_gbp=("positive_value", "sum"),
    )
    table["share_lines_pct"] = 100 * table["lines"] / table["lines"].sum()
    return table


def e3_sale_profile(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sales = data[data["record_type"] == "sale"]
    rows = {}
    for channel, group in sales.groupby("channel"):
        quantity, price = group["Quantity"], group["UnitPrice"]
        rows[channel] = {
            "lines": len(group),
            "qty_mode": quantity.mode().iloc[0],
            "qty_share_eq_1_pct": 100 * (quantity == 1).mean(),
            **{f"qty_p{int(q * 100)}": quantity.quantile(q) for q in QUANTILES},
            "price_mean": price.mean(),
            **{f"price_p{int(q * 100)}": price.quantile(q) for q in (0.25, 0.5, 0.75)},
        }
    profile = pd.DataFrame(rows).T
    positive = data[(data["Quantity"] > 0) & (data["UnitPrice"] > 0)].assign(
        has_id=lambda f: f["customer"].notna())
    means = pd.DataFrame({
        "mean_price_all_positive_lines": positive.groupby("has_id")["UnitPrice"].mean(),
        "mean_price_sale_only": sales.assign(has_id=sales["customer"].notna())
        .groupby("has_id")["UnitPrice"].mean(),
    })
    per_stock = sales.groupby(["stock", "baseline_group"])["UnitPrice"].agg(["size", "median"]).unstack()
    both = per_stock[(per_stock["size"] >= MIN_CHANNEL_SAMPLES).all(axis=1)]
    ratio = both["median"]["retail"] / both["median"]["identified"]
    same_stock = pd.DataFrame({
        "stocks_compared": [len(ratio)],
        "ratio_p25": [ratio.quantile(0.25)], "ratio_median": [ratio.median()],
        "ratio_p75": [ratio.quantile(0.75)],
        "share_retail_higher_pct": [100 * (ratio > 1).mean()],
    }).set_index("stocks_compared")
    return profile, means, same_stock


def e4_record_types(data: pd.DataFrame) -> pd.DataFrame:
    return pd.crosstab(data["record_type"], data["channel"], margins=True, margins_name="total")


def e5_history_depth(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    sales = data[data["record_type"] == "sale"]
    identified = sales[sales["baseline_group"] == "identified"]
    invoices_per_customer = identified.groupby("customer")["invoice"].nunique()
    customers = pd.DataFrame({
        "customers": [len(invoices_per_customer)],
        **{f"invoices_p{int(q * 100)}": [invoices_per_customer.quantile(q)] for q in (0.25, 0.5, 0.75, 0.95)},
        "share_customers_ge5_invoices_pct": [100 * (invoices_per_customer >= MIN_CUSTOMER_INVOICES).mean()],
    }).set_index("customers")
    pair_n = identified.groupby(["customer", "stock"])["invoice"].transform("size")
    customer_n = identified.groupby("customer")["invoice"].transform("size")
    stock_n = sales.groupby(["baseline_group", "stock"])["invoice"].transform("size")
    level = _select(
        [pair_n >= MIN_CUSTOMER_STOCK_LINES, customer_n >= MIN_CUSTOMER_LINES,
         stock_n.loc[identified.index] >= MIN_CHANNEL_SAMPLES],
        ["customer_stock", "customer", "channel_stock"], default="insufficient_history",
    )
    retail = sales[sales["baseline_group"] == "retail"]
    retail_level = np.where(stock_n.loc[retail.index] >= MIN_CHANNEL_SAMPLES,
                            "channel_stock", "insufficient_history")
    coverage = pd.concat([
        pd.Series(level).value_counts(normalize=True).rename("identified_pct"),
        pd.Series(retail_level).value_counts(normalize=True).rename("retail_pct"),
    ], axis=1).mul(100)
    coverage.index.name = "quantity_baseline_level"
    return customers, coverage


def _stock_sale_price(data: pd.DataFrame) -> pd.Series:
    return data[data["record_type"] == "sale"].groupby("stock")["UnitPrice"].median()


def e6_inventory(data: pd.DataFrame) -> pd.DataFrame:
    inventory = data[data["record_type"] == "inventory_adjustment"]
    value = inventory["Quantity"].abs() * inventory["stock"].map(_stock_sale_price(data))
    return pd.DataFrame({
        "lines": [len(inventory)],
        "negative_qty_lines": [(inventory["Quantity"] < 0).sum()],
        "value_unknown": [value.isna().sum()],
        **{f"value_p{int(q * 100)}": [value.quantile(q)] for q in (0.5, 0.9, 0.99)},
        "ge_100_gbp": [(value >= 100).sum()], "ge_1000_gbp": [(value >= 1000).sum()],
        "total_estimated_gbp": [value.sum()],
    }).set_index("lines")


def e7_zero_mad(data: pd.DataFrame) -> pd.DataFrame:
    identified = data[(data["record_type"] == "sale") & (data["baseline_group"] == "identified")]
    log_price = np.log(identified["UnitPrice"])
    keys = [identified["customer"], identified["stock"]]
    median = log_price.groupby(keys).transform("median")
    mad = (log_price - median).abs().groupby(keys).transform("median")
    size = log_price.groupby(keys).transform("size")
    eligible = size >= MIN_CUSTOMER_STOCK_LINES
    pairs = pd.DataFrame({"mad": mad[eligible], "customer": identified["customer"][eligible],
                          "stock": identified["stock"][eligible]}).drop_duplicates(["customer", "stock"])
    return pd.DataFrame({
        "pairs_ge5_lines": [len(pairs)],
        "pairs_zero_mad_pct": [100 * (pairs["mad"] == 0).mean()],
        "lines_in_zero_mad_pairs_pct": [100 * (mad[eligible] == 0).mean()],
    }).set_index("pairs_ge5_lines")


def e8_returns(data: pd.DataFrame) -> pd.DataFrame:
    identified = data[data["customer"].notna()]
    bought = identified[identified["record_type"] == "sale"].groupby(["customer", "stock"])["Quantity"].sum()
    cancels = identified[identified["record_type"] == "cancellation"]
    returned = cancels.groupby(["customer", "stock"])["Quantity"].sum().abs()
    pairs = pd.concat([bought.rename("bought"), returned.rename("returned")], axis=1).fillna(0)
    unmatched = pairs[pairs["returned"] > pairs["bought"]]
    lines = cancels.set_index(["customer", "stock"]).loc[lambda f: f.index.isin(unmatched.index)]
    return pd.DataFrame({
        "cancel_pairs": [len(returned)],
        "unmatched_pairs": [len(unmatched)],
        "unmatched_pairs_without_purchase": [(unmatched["bought"] == 0).sum()],
        "unmatched_cancel_lines": [len(lines)],
        "unmatched_value_gbp": [lines["line_value"].abs().sum()],
    }).set_index("cancel_pairs")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Raw Online Retail CSV")
    args = parser.parse_args(argv)
    data = annotate(load_dedup(args.input))
    print(f"Rows after exact deduplication: {len(data):,}")
    _section("E1. Dòng DOT theo có/thiếu CustomerID", e1_dot(data))
    _section("E2. Quy mô theo kênh", e2_channels(data))
    profile, means, same_stock = e3_sale_profile(data)
    _section("E3a. Hồ sơ dòng sale theo kênh", profile)
    _section("E3b. Đơn giá trung bình: mọi dòng dương vs chỉ dòng sale", means)
    _section("E3c. Tỷ lệ giá trung vị retail/identified trên cùng StockCode", same_stock)
    _section("E4. record_type × channel (số dòng)", e4_record_types(data))
    customers, coverage = e5_history_depth(data)
    _section("E5a. Độ sâu lịch sử khách định danh", customers)
    _section("E5b. Cấp baseline quantity khả dụng (% dòng sale)", coverage)
    _section("E6. Giá trị ước tính dòng kiểm kho", e6_inventory(data))
    _section("E7. MAD log giá bằng 0 ở customer × stock", e7_zero_mad(data))
    _section("E8. Hủy vượt mua theo customer × stock", e8_returns(data))


if __name__ == "__main__":
    main()
