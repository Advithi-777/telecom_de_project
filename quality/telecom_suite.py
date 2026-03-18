"""
Telecom Data Quality Suite
===========================
Runs data quality checks on Snowflake Gold layer tables.
Checks include:
  - Row count thresholds
  - Null value checks
  - Value range validations
  - Accepted value checks
  - Unique column checks

Usage:
    pip install snowflake-connector-python pandas pyarrow python-dotenv
    python quality/telecom_suite.py
"""

import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "mshkneu-vq44359"),
    "user": os.getenv("SNOWFLAKE_USER", "ADVITHI"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": "TELECOM_WH",
    "database": "TELECOM_DB",
    "schema": "SILVER_GOLD",
}


def get_df(query):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    conn.close()
    return df


def check(results, name, condition, description):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {name} — {description}")
    results.append(condition)
    return condition


def run_quality_checks():
    print("=" * 60)
    print("Telecom Data Quality Suite")
    print("=" * 60)

    print("\nLoading tables from Snowflake...")
    customers = get_df("SELECT * FROM SILVER_GOLD.CUSTOMER_MONTHLY_SUMMARY")
    churn = get_df("SELECT * FROM SILVER_GOLD.CHURN_RISK_SCORE")
    tower = get_df("SELECT * FROM SILVER_GOLD.TOWER_PERFORMANCE")

    print(f"  customer_monthly_summary : {len(customers):,} rows")
    print(f"  churn_risk_score         : {len(churn):,} rows")
    print(f"  tower_performance        : {len(tower):,} rows")

    results = []

    # ── Customer Monthly Summary Checks ──────────────────────────────────────
    print("\ncustomer_monthly_summary:")
    check(results, "row_count",
          len(customers) >= 4000,
          f"{len(customers):,} rows >= 4000")
    check(results, "no_null_ids",
          customers["CUSTOMER_ID"].isnull().sum() == 0,
          "no null customer_ids")
    check(results, "unique_ids",
          customers["CUSTOMER_ID"].nunique() == len(customers),
          "customer_ids are unique")
    check(results, "valid_plans",
          customers["PLAN"].isin(
              ["Basic", "Standard", "Premium", "Business"]
          ).all(),
          "valid plan values")
    check(results, "valid_regions",
          customers["REGION"].isin([
              "London", "Birmingham", "Leeds", "Glasgow", "Manchester",
              "Bristol", "Sheffield", "Liverpool", "Newcastle", "Oxford"
          ]).all(),
          "valid regions")
    check(results, "charge_range",
          customers["MONTHLY_CHARGE"].between(5, 200).all(),
          "monthly charge between 5-200")

    # ── Churn Risk Score Checks ───────────────────────────────────────────────
    print("\nchurn_risk_score:")
    check(results, "row_count",
          len(churn) >= 4000,
          f"{len(churn):,} rows >= 4000")
    check(results, "no_null_ids",
          churn["CUSTOMER_ID"].isnull().sum() == 0,
          "no null customer_ids")
    check(results, "score_range",
          churn["CHURN_RISK_SCORE"].between(0, 100).all(),
          "churn score between 0-100")
    check(results, "valid_labels",
          churn["CHURN_RISK_LABEL"].isin(
              ["Low", "Medium", "High", "Critical"]
          ).all(),
          "valid risk labels")

    # ── Tower Performance Checks ──────────────────────────────────────────────
    print("\ntower_performance:")
    check(results, "row_count",
          len(tower) >= 100,
          f"{len(tower):,} rows >= 100")
    check(results, "no_null_ids",
          tower["TOWER_ID"].isnull().sum() == 0,
          "no null tower_ids")
    check(results, "valid_ratings",
          tower["TOWER_HEALTH_RATING"].isin(
              ["Excellent", "Good", "Fair", "Poor"]
          ).all(),
          "valid health ratings")
    check(results, "degraded_pct",
          tower["DEGRADED_PCT"].between(0, 100).all(),
          "degraded pct between 0-100")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("QUALITY CHECK SUMMARY")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"  TOTAL: {passed}/{total} checks passed")
    if passed == total:
        print("  ALL CHECKS PASSED")
    else:
        print(f"  {total - passed} CHECKS FAILED")
    print("=" * 60)

    if passed != total:
        raise Exception(f"{total - passed} quality checks failed!")


if __name__ == "__main__":
    run_quality_checks()

