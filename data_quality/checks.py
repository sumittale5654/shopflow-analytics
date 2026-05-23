"""
ShopFlow — Data Quality Checks
Runs assertions against the processed mart tables.
Raises an exception if any CRITICAL check fails (which fails the Airflow task).
WARNING-level failures are logged but don't stop the pipeline.
"""

import os
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd
from loguru import logger

from ingestion.config import MARTS_DIR, PROCESSED_DIR


@dataclass
class CheckResult:
    name:     str
    passed:   bool
    severity: str   # "critical" or "warning"
    message:  str   = ""
    details:  dict  = field(default_factory=dict)


def read_parquet_dir(directory: str) -> pd.DataFrame:
    dfs = []
    for root, _, files in os.walk(directory):
        for f in files:
            if f.endswith(".parquet"):
                dfs.append(pd.read_parquet(os.path.join(root, f)))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── Individual checks ─────────────────────────────────────────────────────────

def check_row_count(df: pd.DataFrame, name: str, min_rows: int, severity: str = "critical") -> CheckResult:
    count = len(df)
    passed = count >= min_rows
    return CheckResult(
        name=f"{name}_min_row_count",
        passed=passed,
        severity=severity,
        message=f"Expected >= {min_rows} rows, got {count}",
        details={"actual": count, "minimum": min_rows}
    )


def check_no_nulls(df: pd.DataFrame, table: str, columns: list[str], severity: str = "critical") -> list[CheckResult]:
    results = []
    for col in columns:
        if col not in df.columns:
            results.append(CheckResult(
                name=f"{table}_{col}_exists",
                passed=False,
                severity=severity,
                message=f"Column '{col}' not found in {table}"
            ))
            continue
        null_count = df[col].isna().sum()
        results.append(CheckResult(
            name=f"{table}_{col}_no_nulls",
            passed=null_count == 0,
            severity=severity,
            message=f"{null_count} null values in {col}",
            details={"null_count": int(null_count)}
        ))
    return results


def check_no_negative(df: pd.DataFrame, table: str, column: str, severity: str = "warning") -> CheckResult:
    if column not in df.columns:
        return CheckResult(name=f"{table}_{column}_no_negative", passed=True,
                           severity=severity, message="Column not present")
    neg = (df[column] < 0).sum()
    return CheckResult(
        name=f"{table}_{column}_no_negative",
        passed=neg == 0,
        severity=severity,
        message=f"{neg} negative values in {column}",
        details={"negative_count": int(neg)}
    )


def check_unique(df: pd.DataFrame, table: str, column: str, severity: str = "critical") -> CheckResult:
    if column not in df.columns:
        return CheckResult(name=f"{table}_{column}_unique", passed=False,
                           severity=severity, message=f"Column {column} not found")
    dupes = df[column].duplicated().sum()
    return CheckResult(
        name=f"{table}_{column}_unique",
        passed=dupes == 0,
        severity=severity,
        message=f"{dupes} duplicate values in {column}",
        details={"duplicate_count": int(dupes)}
    )


def check_value_set(df: pd.DataFrame, table: str, column: str,
                     valid_values: set, severity: str = "warning") -> CheckResult:
    if column not in df.columns:
        return CheckResult(name=f"{table}_{column}_valid_values", passed=True,
                           severity=severity, message="Column not present")
    invalid = ~df[column].isin(valid_values)
    invalid_count = invalid.sum()
    return CheckResult(
        name=f"{table}_{column}_valid_values",
        passed=invalid_count == 0,
        severity=severity,
        message=f"{invalid_count} rows with invalid {column} values",
        details={"invalid_count": int(invalid_count), "valid_set": list(valid_values)}
    )


# ── Check suites ──────────────────────────────────────────────────────────────

def check_daily_sales() -> list[CheckResult]:
    df = read_parquet_dir(f"{MARTS_DIR}/daily_sales")
    return [
        check_row_count(df, "daily_sales", 100),
        *check_no_nulls(df, "daily_sales", ["sale_date", "category", "gross_revenue"]),
        check_no_negative(df, "daily_sales", "gross_revenue"),
        check_no_negative(df, "daily_sales", "gross_profit"),
    ]


def check_product_performance() -> list[CheckResult]:
    df = read_parquet_dir(f"{MARTS_DIR}/product_performance")
    return [
        check_row_count(df, "product_performance", 50),
        check_unique(df, "product_performance", "product_id"),
        *check_no_nulls(df, "product_performance", ["product_id", "product_name", "total_revenue"]),
        check_no_negative(df, "product_performance", "total_revenue"),
    ]


def check_customer_ltv() -> list[CheckResult]:
    df = read_parquet_dir(f"{MARTS_DIR}/customer_ltv")
    return [
        check_row_count(df, "customer_ltv", 500),
        check_unique(df, "customer_ltv", "customer_id"),
        *check_no_nulls(df, "customer_ltv", ["customer_id", "email", "lifetime_value"]),
        check_value_set(df, "customer_ltv", "ltv_segment", {"VIP", "High", "Medium", "Low"}),
    ]


# ── Runner ────────────────────────────────────────────────────────────────────

def run_checks() -> None:
    logger.info("=== ShopFlow Data Quality Checks ===")

    all_checks: list[CheckResult] = []
    all_checks.extend(check_daily_sales())
    all_checks.extend(check_product_performance())
    all_checks.extend(check_customer_ltv())

    passed_count = sum(1 for c in all_checks if c.passed)
    failed_critical = [c for c in all_checks if not c.passed and c.severity == "critical"]
    failed_warning  = [c for c in all_checks if not c.passed and c.severity == "warning"]

    logger.info(f"Results: {passed_count}/{len(all_checks)} checks passed")

    for check in all_checks:
        icon = "✓" if check.passed else ("✗" if check.severity == "critical" else "⚠")
        msg = f"  {icon} [{check.severity.upper()}] {check.name}: {check.message}"
        if check.passed:
            logger.success(msg)
        elif check.severity == "critical":
            logger.error(msg)
        else:
            logger.warning(msg)

    if failed_warning:
        logger.warning(f"{len(failed_warning)} warning-level checks failed (pipeline continues)")

    if failed_critical:
        raise ValueError(
            f"DATA QUALITY FAILURE: {len(failed_critical)} critical checks failed: "
            + ", ".join(c.name for c in failed_critical)
        )

    logger.success("=== All critical checks passed ===")


if __name__ == "__main__":
    run_checks()
