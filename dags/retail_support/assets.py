"""Logical datasets; exact snapshot paths travel in event metadata."""

from airflow.sdk import Asset

CURATED_TRANSACTIONS_ASSET = Asset(
    uri="file:///opt/airflow/data/curated/transactions",
    name="curated_transactions",
)
RFM_CUSTOMERS_ASSET = Asset(
    uri="file:///opt/airflow/data/analytics/rfm_daily/customers",
    name="rfm_customers",
)
FLAGGED_ANOMALIES_ASSET = Asset(
    uri="file:///opt/airflow/data/audit/anomalies/flagged_orders",
    name="flagged_anomalies",
)

RETAIL_SNAPSHOT_ASSET = Asset(
    uri="file:///opt/airflow/data/analytics/retail_snapshot",
    name="retail_snapshot",
)

PUBLISHED_ASSETS = (
    CURATED_TRANSACTIONS_ASSET,
    RFM_CUSTOMERS_ASSET,
    FLAGGED_ANOMALIES_ASSET,
    RETAIL_SNAPSHOT_ASSET,
)

COMPLETION_ASSETS = (CURATED_TRANSACTIONS_ASSET, RETAIL_SNAPSHOT_ASSET)
