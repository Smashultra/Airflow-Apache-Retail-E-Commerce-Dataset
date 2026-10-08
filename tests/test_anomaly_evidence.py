"""Channel classification used by the anomaly evidence tables."""

import pytest

pd = pytest.importorskip("pandas")

from anomaly_evidence import invoice_channels  # noqa: E402


def test_invoice_channels_priority():
    frame = pd.DataFrame(
        [
            ("1", "DOT", 1, 5.0, "12345"),
            ("1", "100", 2, 1.0, "12345"),
            ("2", "DOT", 1, 5.0, None),
            ("2", "100", 1, 0.0, None),
            ("3", "100", 1, 2.0, None),
            ("4", "100", -5, 0.0, None),
        ],
        columns=["InvoiceNo", "StockCode", "Quantity", "UnitPrice", "CustomerID"],
    )

    channels = invoice_channels(frame)

    assert channels.to_dict() == {
        "1": "identified",
        "2": "retail_web",
        "3": "retail_other",
        "4": "internal",
    }
