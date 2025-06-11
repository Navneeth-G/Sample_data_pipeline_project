from datetime import datetime
from pipeline_logic_scripts.snowflake_funcs.snowflake_query_client import SnowflakeQueryClient
from utils.log_utils import LogBlock
from typing import Optional

logger = LogBlock(logger_name="data_pipeline", max_depth=3)


def create_table_if_not_exists(
    snowflake_client: SnowflakeQueryClient,
    create_query: str,
    database: str,
    schema: str
) -> dict:
    """
    Executes a CREATE TABLE IF NOT EXISTS statement in Snowflake.
    Logs using a consistent structure for traceability.

    Args:
        snowflake_client (SnowflakeQueryClient): Snowflake client instance.
        create_query (str): Full SQL statement.
        database (str): Target Snowflake database.
        schema (str): Target schema.

    Returns:
        dict: {
            "query_id": str,
            "executed": True
        }

    Raises:
        RuntimeError: If execution fails.
    """
    key = "CREATE_TABLE_IF_NOT_EXISTS"

    logger.info(
        key=key,
        message=f"STATUS: STARTED\nQUERY:\n{create_query}"
    )

    try:
        result = snowflake_client.execute_control_command(
            query=create_query,
            database=database,
            schema=schema
        )

        logger.info(
            key=key,
            message=f"STATUS: COMPLETED\nQUERY ID: {result['query_id']}\nQUERY:\n{create_query}"
        )
        return {
            "query_id": result["query_id"],
            "executed": True
        }

    except Exception as error:
        logger.error(
            key=key,
            message=f"STATUS: FAILED\nQUERY:\n{create_query}\nERROR: {error}"
        )
        raise


def count_records_by_pipeline_status(
    table_name: str,
    database: str,
    schema: str,
    pipeline_status: str,
    snowflake_client: SnowflakeQueryClient,
    logger: LogBlock
) -> dict:
    """
    Executes a COUNT(*) query filtered by pipeline_status on a given table.
    Logs execution lifecycle and SQL details in a consistent structure.

    Args:
        table_name (str): Target table name.
        database (str): Snowflake database name.
        schema (str): Snowflake schema name.
        pipeline_status (str): Filter value (e.g., 'completed').
        snowflake_client (SnowflakeQueryClient): Active Snowflake client.
        logger (LogBlock): Project-standard logging instance.

    Returns:
        dict: {
            "query_id": str,
            "row_count": int
        }

    Raises:
        RuntimeError: On execution failure.
    """
    key = "COUNT_PIPELINE_STATUS"
    query = f"SELECT COUNT(*) FROM {table_name} WHERE pipeline_status = %(pipeline_status)s"

    logger.info(
        key=key,
        message=(
            f"STATUS: STARTED\n"
            f"PARAMS: pipeline_status = '{pipeline_status}'\n"
            f"QUERY:\n{query}"
        )
    )

    try:
        result = snowflake_client.execute_scalar_query(
            query=query,
            database=database,
            schema=schema,
            query_params={"pipeline_status": pipeline_status}
        )

        logger.info(
            key=key,
            message=(
                f"STATUS: COMPLETED\n"
                f"COUNT: {result['data']}\n"
                f"QUERY ID: {result['query_id']}\n"
                f"QUERY:\n{query}"
            )
        )

        return {
            "query_id": result["query_id"],
            "row_count": result["data"]
        }

    except Exception as error:
        logger.error(
            key=key,
            message=(
                f"STATUS: FAILED\n"
                f"ERROR: {error}\n"
                f"QUERY:\n{query}"
            )
        )
        raise

def get_oldest_record_by_status(
    table_name: str,
    database: str,
    schema: str,
    pipeline_status: str,
    snowflake_client: SnowflakeQueryClient,
    logger: LogBlock
) -> dict:
    """
    Retrieves the oldest record (by query_window_start_ts) from a table
    for a specific pipeline_status (e.g., 'pending', 'failed', 'in_progress').

    Timestamp fields are converted to ISO 8601 strings for downstream compatibility.

    Args:
        table_name (str): Name of the target table.
        database (str): Snowflake database name.
        schema (str): Snowflake schema name.
        pipeline_status (str): Must be one of 'pending', 'failed', 'in_progress'.
        snowflake_client (SnowflakeQueryClient): Snowflake connection client.
        logger (LogBlock): Structured logger instance.

    Returns:
        dict: {
            "query_id": str,
            "record": dict or None
        }

    Raises:
        RuntimeError: If the query fails.
    """
    status_key = pipeline_status.upper()
    key = f"PICK_OLDEST_{status_key}"
    query = f"""
        SELECT * FROM {table_name}
        WHERE pipeline_status = %(pipeline_status)s
        ORDER BY query_window_start_ts ASC
        LIMIT 1
    """

    logger.info(
        key=key,
        message=(
            f"STATUS: STARTED\n"
            f"FILTER: pipeline_status = '{pipeline_status}'\n"
            f"QUERY:\n{query.strip()}"
        )
    )

    try:
        result = snowflake_client.fetch_all_rows_as_dataframe(
            query=query,
            database=database,
            schema=schema,
            query_params={"pipeline_status": pipeline_status}
        )

        df = result["data"]
        query_id = result["query_id"]

        if df.empty:
            logger.info(
                key=key,
                message=(
                    f"STATUS: COMPLETED\n"
                    f"QUERY ID: {query_id}\n"
                    f"RESULT: No matching records\n"
                    f"FILTER: pipeline_status = '{pipeline_status}'\n"
                    f"QUERY:\n{query.strip()}"
                )
            )
            return {"query_id": query_id, "record": None}

        record_dict = {
            col: val.isoformat() if isinstance(val, datetime) else val
            for col, val in df.iloc[0].items()
        }

        logger.info(
            key=key,
            message=(
                f"STATUS: COMPLETED\n"
                f"QUERY ID: {query_id}\n"
                f"RECORD PICKED:\n{record_dict}\n"
                f"FILTER: pipeline_status = '{pipeline_status}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )

        return {
            "query_id": query_id,
            "record": record_dict
        }

    except Exception as error:
        logger.error(
            key=key,
            message=(
                f"STATUS: FAILED\n"
                f"ERROR: {error}\n"
                f"FILTER: pipeline_status = '{pipeline_status}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        raise

def get_latest_record_by_status(
    table_name: str,
    database: str,
    schema: str,
    pipeline_status: str,
    snowflake_client: SnowflakeQueryClient,
    logger: LogBlock
) -> dict:
    """
    Retrieves the latest record (by query_window_start_ts DESC)
    from a table for a given pipeline_status.

    Datetime fields are converted to ISO 8601 strings for compatibility.

    Args:
        table_name (str): Table to query.
        database (str): Snowflake database name.
        schema (str): Schema name.
        pipeline_status (str): Filter value (e.g., 'completed', 'failed').
        snowflake_client (SnowflakeQueryClient): Shared connection client.
        logger (LogBlock): Logger instance.

    Returns:
        dict: {
            "query_id": str,
            "record": dict or None
        }

    Raises:
        RuntimeError: On query failure.
    """
    status_key = pipeline_status.upper()
    key = f"PICK_LATEST_{status_key}"
    query = f"""
        SELECT * FROM {table_name}
        WHERE pipeline_status = %(pipeline_status)s
        ORDER BY query_window_start_ts DESC
        LIMIT 1
    """

    logger.info(
        key=key,
        message=(
            f"STATUS: STARTED\n"
            f"FILTER: pipeline_status = '{pipeline_status}'\n"
            f"QUERY:\n{query.strip()}"
        )
    )

    try:
        result = snowflake_client.fetch_all_rows_as_dataframe(
            query=query,
            database=database,
            schema=schema,
            query_params={"pipeline_status": pipeline_status}
        )

        df = result["data"]
        query_id = result["query_id"]

        if df.empty:
            logger.info(
                key=key,
                message=(
                    f"STATUS: COMPLETED\n"
                    f"QUERY ID: {query_id}\n"
                    f"RESULT: No matching records\n"
                    f"FILTER: pipeline_status = '{pipeline_status}'\n"
                    f"QUERY:\n{query.strip()}"
                )
            )
            return {"query_id": query_id, "record": None}

        record_dict = {
            col: val.isoformat() if isinstance(val, datetime) else val
            for col, val in df.iloc[0].items()
        }

        logger.info(
            key=key,
            message=(
                f"STATUS: COMPLETED\n"
                f"QUERY ID: {query_id}\n"
                f"RECORD PICKED:\n{record_dict}\n"
                f"FILTER: pipeline_status = '{pipeline_status}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )

        return {
            "query_id": query_id,
            "record": record_dict
        }

    except Exception as error:
        logger.error(
            key=key,
            message=(
                f"STATUS: FAILED\n"
                f"ERROR: {error}\n"
                f"FILTER: pipeline_status = '{pipeline_status}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        raise

def get_discontinuous_query_windows(
    day: str,
    pipeline_name: str,
    index_name: str,
    table_name: str,
    database: str,
    schema: str,
    snowflake_client: SnowflakeQueryClient,
    logger: LogBlock
) -> dict:
    """
    Checks for non-continuous query windows in a Snowflake table for a specific day,
    pipeline, and index. Uses SQL only (no row-by-row Python comparisons).

    Args:
        day (str): Date string (e.g., '2025-06-11').
        pipeline_name (str): Name of the pipeline.
        index_name (str): Name of the index.
        table_name (str): Target Snowflake table.
        database (str): Database name.
        schema (str): Schema name.
        snowflake_client (SnowflakeQueryClient): Connection client.
        logger (LogBlock): Logging instance.

    Returns:
        dict: {
            "query_id": str,
            "is_continuous": bool,
            "discontinuities": list[dict]
        }
    """
    key = "CHECK_DISCONTINUOUS_QUERY_WINDOWS"

    query = f"""
        WITH ordered_windows AS (
            SELECT
                query_window_start_ts,
                query_window_end_ts,
                LAG(query_window_end_ts) OVER (
                    ORDER BY query_window_start_ts
                ) AS prev_end_ts
            FROM {table_name}
            WHERE DATE(query_window_start_ts) = %(day)s
              AND pipeline_name = %(pipeline_name)s
              AND index_name = %(index_name)s
        )
        SELECT
            prev_end_ts AS missing_query_window_start_ts,
            query_window_start_ts AS missing_query_window_end_ts
        FROM ordered_windows
        WHERE prev_end_ts IS NOT NULL
          AND query_window_start_ts != prev_end_ts
        ORDER BY missing_query_window_start_ts
    """

    logger.info(
        key=key,
        message=(
            f"STATUS: STARTED\n"
            f"FILTER: pipeline_status = 'N/A', pipeline_name = '{pipeline_name}', index_name = '{index_name}', day = '{day}'\n"
            f"QUERY:\n{query.strip()}"
        )
    )

    try:
        result = snowflake_client.fetch_all_rows_as_dataframe(
            query=query,
            database=database,
            schema=schema,
            query_params={
                "day": day,
                "pipeline_name": pipeline_name,
                "index_name": index_name
            }
        )

        df = result["data"]
        query_id = result["query_id"]

        if df.empty:
            logger.info(
                key=key,
                message=(
                    f"STATUS: COMPLETED\n"
                    f"QUERY ID: {query_id}\n"
                    f"RESULT: Query windows are continuous\n"
                    f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}', day = '{day}'\n"
                    f"QUERY:\n{query.strip()}"
                )
            )
            return {
                "query_id": query_id,
                "is_continuous": True,
                "discontinuities": []
            }

        gaps = [
            {
                "missing_query_window_start_ts": r["missing_query_window_start_ts"].isoformat()
                if isinstance(r["missing_query_window_start_ts"], datetime) else r["missing_query_window_start_ts"],

                "missing_query_window_end_ts": r["missing_query_window_end_ts"].isoformat()
                if isinstance(r["missing_query_window_end_ts"], datetime) else r["missing_query_window_end_ts"]
            }
            for _, r in df.iterrows()
        ]

        logger.info(
            key=key,
            message=(
                f"STATUS: COMPLETED\n"
                f"QUERY ID: {query_id}\n"
                f"DISCONTINUITIES FOUND: {len(gaps)}\n"
                f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}', day = '{day}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )

        return {
            "query_id": query_id,
            "is_continuous": False,
            "discontinuities": gaps
        }

    except Exception as error:
        logger.error(
            key=key,
            message=(
                f"STATUS: FAILED\n"
                f"ERROR: {error}\n"
                f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}', day = '{day}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        raise

def find_overlapping_query_windows(
    client: SnowflakeQueryClient,
    database: str,
    schema: str,
    table_name: str,
    pipeline_name: str,
    index_name: str,
    date_str: str,
) -> dict:
    """
    Identifies overlapping query windows within a given day for a specific pipeline and index.

    A query window is considered overlapping if its time range intersects with another record's window.
    This function helps detect duplicate or conflicting processing ranges.

    Args:
        client (SnowflakeQueryClient): An active SnowflakeQueryClient instance.
        database (str): Target Snowflake database name.
        schema (str): Target schema name.
        table_name (str): Name of the table to check.
        pipeline_name (str): Value to filter `pipeline_name` column.
        index_name (str): Value to filter `index_name` column.
        date_str (str): Date in 'YYYY-MM-DD' format to define the day being checked.

    Returns:
        dict:
            {
                "query_id": str,
                "data": pd.DataFrame of overlaps (may be empty),
            }

    Raises:
        RuntimeError: If query execution fails.
    """
    logger = LogBlock(logger_name="data_pipeline", max_depth=6)
    key = "FIND_OVERLAPPING_WINDOWS"

    start_ts = f"{date_str} 00:00:00"
    end_expr = f"DATEADD(day, 1, '{date_str}')"

    logger.log_start(key=key, message=f"Checking overlaps for date: {date_str}")

    query = f"""
    WITH filtered_day_data AS (
        SELECT *
        FROM {table_name}
        WHERE pipeline_name = %(pipeline_name)s
          AND index_name = %(index_name)s
          AND query_window_start_ts < {end_expr}
          AND query_window_end_ts > '{start_ts}'
    )
    SELECT
        t1.query_window_start_ts AS source_window_start_ts,
        t1.query_window_end_ts AS source_window_end_ts,
        t2.query_window_start_ts AS overlaps_with_start_ts,
        t2.query_window_end_ts AS overlaps_with_end_ts
    FROM filtered_day_data t1
    INNER JOIN filtered_day_data t2
      ON t1.query_window_start_ts < t2.query_window_end_ts
     AND t1.query_window_end_ts > t2.query_window_start_ts
     AND t1.query_window_start_ts != t2.query_window_start_ts
    ORDER BY source_window_start_ts, overlaps_with_start_ts;
    """

    try:
        result = client.fetch_all_rows_as_dataframe(
            query=query,
            database=database,
            schema=schema,
            query_params={
                "pipeline_name": pipeline_name,
                "index_name": index_name
            }
        )
        df = result["data"]
        logger.log_complete(
            key=key,
            message=(
                f"STATUS: COMPLETED\n"
                f"QUERY ID: {result['query_id']}\n"
                f"RECORDS FOUND: {len(df)}\n"
                f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}', date = '{date_str}'\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        return result
    except Exception as e:
        logger.log_failure(
            key=key,
            message=(
                f"STATUS: FAILED\n"
                f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}', date = '{date_str}'\n"
                f"ERROR: {str(e)}\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        raise
