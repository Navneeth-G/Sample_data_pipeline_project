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






