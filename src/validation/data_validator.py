from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct
from src.utils.logger import logger


class DataValidator:
    """
    Data Quality & Validation layer for the E-Commerce Data Engineering Pipeline.
    Validates:
    - Primary Key Uniqueness
    - Mandatory Column Non-Null checks
    - Value Range boundaries (quantity > 0, price > 0)
    - Referential Integrity between Fact and Dimension tables
    """

    def __init__(self):
        self.validation_results = []

    def check_unique_primary_key(self, df: DataFrame, pk_col: str, table_name: str) -> Dict[str, Any]:
        """Checks if primary key column contains duplicate values."""
        total_count = df.count()
        distinct_count = df.select(pk_col).distinct().count()
        duplicate_count = total_count - distinct_count

        status = "PASSED" if duplicate_count == 0 else "FAILED"
        result = {
            "table": table_name,
            "check": f"Duplicate Primary Key Check ({pk_col})",
            "total_records": total_count,
            "failed_records": duplicate_count,
            "status": status
        }
        self.validation_results.append(result)
        logger.info(f"[{status}] {table_name}.{pk_col} Primary Key Check: {duplicate_count} duplicate(s) found out of {total_count} records.")
        return result

    def check_non_null_columns(self, df: DataFrame, mandatory_cols: List[str], table_name: str) -> Dict[str, Any]:
        """Checks if mandatory columns contain null or empty string values."""
        total_count = df.count()
        failed_count = 0

        for column in mandatory_cols:
            null_cnt = df.filter(col(column).isNull() | (col(column) == "")).count()
            if null_cnt > 0:
                failed_count += null_cnt
                logger.warning(f"  -> Column '{column}' in {table_name} has {null_cnt} null/empty values.")

        status = "PASSED" if failed_count == 0 else "FAILED"
        result = {
            "table": table_name,
            "check": f"Mandatory Non-Null Check ({', '.join(mandatory_cols)})",
            "total_records": total_count,
            "failed_records": failed_count,
            "status": status
        }
        self.validation_results.append(result)
        logger.info(f"[{status}] {table_name} Mandatory Non-Null Check: {failed_count} violation(s).")
        return result

    def check_numeric_bounds(self, df: DataFrame, col_name: str, min_val: float, table_name: str) -> Dict[str, Any]:
        """Validates that numerical column values meet minimum thresholds (e.g. quantity > 0)."""
        total_count = df.count()
        invalid_cnt = df.filter(col(col_name).isNull() | (col(col_name) <= min_val)).count()

        status = "PASSED" if invalid_cnt == 0 else "FAILED"
        result = {
            "table": table_name,
            "check": f"Value Bound Check ({col_name} > {min_val})",
            "total_records": total_count,
            "failed_records": invalid_cnt,
            "status": status
        }
        self.validation_results.append(result)
        logger.info(f"[{status}] {table_name}.{col_name} Bound Check (> {min_val}): {invalid_cnt} invalid record(s).")
        return result

    def check_referential_integrity(self, fact_df: DataFrame, dim_df: DataFrame, fk_col: str, pk_col: str, fact_table: str, dim_table: str) -> Dict[str, Any]:
        """
        Validates referential integrity: ensures all Foreign Keys in Fact table 
        exist in the corresponding Dimension table.
        """
        total_fact_records = fact_df.count()
        # Left anti join finds foreign keys in fact table that DO NOT exist in dim table
        orphan_df = fact_df.join(dim_df, fact_df[fk_col] == dim_df[pk_col], "left_anti")
        orphan_count = orphan_df.count()

        status = "PASSED" if orphan_count == 0 else "FAILED"
        result = {
            "table": f"{fact_table} -> {dim_table}",
            "check": f"Referential Integrity ({fk_col} -> {pk_col})",
            "total_records": total_fact_records,
            "failed_records": orphan_count,
            "status": status
        }
        self.validation_results.append(result)
        logger.info(f"[{status}] Referential Integrity Check ({fact_table}.{fk_col} -> {dim_table}.{pk_col}): {orphan_count} orphan record(s).")
        return result

    def print_validation_report(self) -> str:
        """Prints a human-readable Data Quality summary report."""
        report = []
        report.append("=" * 80)
        report.append("                    DATA QUALITY & VALIDATION REPORT                   ")
        report.append("=" * 80)
        report.append(f"{'TABLE':<25} | {'CHECK TYPE':<32} | {'FAILED':<7} | {'STATUS'}")
        report.append("-" * 80)

        for res in self.validation_results:
            report.append(f"{res['table']:<25} | {res['check']:<32} | {res['failed_records']:<7} | {res['status']}")

        report.append("=" * 80)
        report_str = "\n".join(report)
        logger.info(f"\n{report_str}")
        return report_str
