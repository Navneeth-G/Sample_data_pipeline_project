from datetime import datetime
from pipeline_logic_scripts.snowflake_funcs.snowflake_query_client import SnowflakeQueryClient
from utils.log_utils import LogBlock

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
