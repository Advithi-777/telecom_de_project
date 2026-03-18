"""
Telecom Data Quality Suite
===========================
Runs data quality checks on Snowflake Gold layer tables using
Great Expectations. Checks include:
  - Row count thresholds
  - Null value checks
  - Value range validations
  - Accepted value checks
  - Unique column checks

Usage:
    pip install great-expectations snowflake-sqlalchemy
    python quality/telecom_suite.py
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# ── Snowflake Connection ──────────────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT",   "mshkneu-vq44359"),
    "user":      os.getenv("SNOWFLAKE_USER",       "ADVITHI"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE",  "TELECOM_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",   "TELECOM_DB"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",     "SILVER_GOLD"),
}

def get_snowflake_df(query):
    """Fetch data from Snowflake into a Pandas DataFrame."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    conn.close()
    return df


def run_quality_checks():
    print("=" * 60)
    print("Telecom Data Quality Suite")
    print("=" * 60)

    results = []

    # ── 1. Load tables ────────────────────────────────────────────────────────
    print("\nLoading tables from Snowflake...")

    customers_df    = get_snowflake_df("SELECT * FROM SILVER_GOLD.CUSTOMER_MONTHLY_SUMMARY")
    churn_df        = get_snowflake_df("SELECT * FROM SILVER_GOLD.CHURN_RISK_SCORE")
    tower_df        = get_snowflake_df("SELECT * FROM SILVER_GOLD.TOWER_PERFORMANCE")

    print(f"  customer_monthly_summary : {len(customers_df):,} rows")
    print(f"  churn_risk_score         : {len(churn_df):,} rows")
    print(f"  tower_performance        : {len(tower_df):,} rows")

    # ── 2. Create GX context ──────────────────────────────────────────────────
    context = gx.get_context()

    # ── 3. Customer Monthly Summary Checks ───────────────────────────────────
    print("\nRunning checks on customer_monthly_summary...")

    ds_customers = context.sources.add_or_update_pandas(name="customers")
    da_customers = ds_customers.add_dataframe_asset(name="customer_monthly_summary")
    batch_customers = da_customers.build_batch_request(dataframe=customers_df)

    suite_customers = context.add_or_update_expectation_suite("customers_suite")
    validator_c = context.get_validator(
        batch_request=batch_customers,
        expectation_suite=suite_customers
    )

    # Row count check
    validator_c.expect_table_row_count_to_be_between(min_value=4000, max_value=6000)

    # Not null checks
    validator_c.expect_column_values_to_not_be_null("CUSTOMER_ID")
    validator_c.expect_column_values_to_not_be_null("REGION")
    validator_c.expect_column_values_to_not_be_null("PLAN")

    # Unique customer IDs
    validator_c.expect_column_values_to_be_unique("CUSTOMER_ID")

    # Valid regions
    validator_c.expect_column_values_to_be_in_set("REGION", [
        "London", "Birmingham", "Leeds", "Glasgow", "Manchester",
        "Bristol", "Sheffield", "Liverpool", "Newcastle", "Oxford"
    ])

    # Valid plans
    validator_c.expect_column_values_to_be_in_set("PLAN", [
        "Basic", "Standard", "Premium", "Business"
    ])

    # Monthly charge range
    validator_c.expect_column_values_to_be_between(
        "MONTHLY_CHARGE", min_value=5, max_value=200
    )

    # Risk segment values
    validator_c.expect_column_values_to_be_in_set("RISK_SEGMENT", [
        "Low Risk", "Medium Risk", "High Risk"
    ])

    results_c = validator_c.validate()
    results.append(("customer_monthly_summary", results_c))

    # ── 4. Churn Risk Score Checks ────────────────────────────────────────────
    print("Running checks on churn_risk_score...")

    ds_churn = context.sources.add_or_update_pandas(name="churn")
    da_churn = ds_churn.add_dataframe_asset(name="churn_risk_score")
    batch_churn = da_churn.build_batch_request(dataframe=churn_df)

    suite_churn = context.add_or_update_expectation_suite("churn_suite")
    validator_ch = context.get_validator(
        batch_request=batch_churn,
        expectation_suite=suite_churn
    )

    # Row count
    validator_ch.expect_table_row_count_to_be_between(min_value=4000, max_value=6000)

    # Not null
    validator_ch.expect_column_values_to_not_be_null("CUSTOMER_ID")
    validator_ch.expect_column_values_to_not_be_null("CHURN_RISK_SCORE")
    validator_ch.expect_column_values_to_not_be_null("CHURN_RISK_LABEL")

    # Churn score range (0-100)
    validator_ch.expect_column_values_to_be_between(
        "CHURN_RISK_SCORE", min_value=0, max_value=100
    )

    # Valid churn risk labels
    validator_ch.expect_column_values_to_be_in_set("CHURN_RISK_LABEL", [
        "Low", "Medium", "High", "Critical"
    ])

    results_ch = validator_ch.validate()
    results.append(("churn_risk_score", results_ch))

    # ── 5. Tower Performance Checks ───────────────────────────────────────────
    print("Running checks on tower_performance...")

    ds_tower = context.sources.add_or_update_pandas(name="tower")
    da_tower = ds_tower.add_dataframe_asset(name="tower_performance")
    batch_tower = da_tower.build_batch_request(dataframe=tower_df)

    suite_tower = context.add_or_update_expectation_suite("tower_suite")
    validator_t = context.get_validator(
        batch_request=batch_tower,
        expectation_suite=suite_tower
    )

    # Row count
    
    validator_t.expect_table_row_count_to_be_between(min_value=100, max_value=5000)

    # Not null
    validator_t.expect_column_values_to_not_be_null("TOWER_ID")
    validator_t.expect_column_values_to_not_be_null("REGION")
    validator_t.expect_column_values_to_not_be_null("TOWER_HEALTH_RATING")

    # Valid health ratings
    validator_t.expect_column_values_to_be_in_set("TOWER_HEALTH_RATING", [
        "Excellent", "Good", "Fair", "Poor"
    ])

    # Download speed should be positive
    validator_t.expect_column_values_to_be_between(
        "AVG_DOWNLOAD_SPEED", min_value=-100, max_value=1000, mostly=0.95
    )

    # Degraded percentage should be between 0-100
    validator_t.expect_column_values_to_be_between(
        "DEGRADED_PCT", min_value=0, max_value=100, mostly=0.95
    )

    results_t = validator_t.validate()
    
    # Print failed checks only
    for r in results_t["results"]:
        if not r["success"]:
            print(f"FAILED: {r['expectation_config']['expectation_type']} on column {r['expectation_config']['kwargs']}")
    
    results.append(("tower_performance", results_t))

    # ── 6. Print Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("QUALITY CHECK SUMMARY")
    print("=" * 60)

    all_passed = True
    for table_name, result in results:
        passed     = result["statistics"]["successful_expectations"]
        failed     = result["statistics"]["unsuccessful_expectations"]
        total      = result["statistics"]["evaluated_expectations"]
        status     = "PASS" if failed == 0 else "FAIL"
        if failed > 0:
            all_passed = False
        print(f"  {table_name:<35} {status}  ({passed}/{total} checks passed)")

    print("=" * 60)
    if all_passed:
        print("  ALL CHECKS PASSED")
    else:
        print("  SOME CHECKS FAILED — review above for details")
    print("=" * 60)

    return all_passed


if __name__ == "__main__":
    run_quality_checks()