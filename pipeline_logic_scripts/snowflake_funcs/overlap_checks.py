from datetime import datetime
from utils.log_utils import LogBlock

def find_overlapping_records_for_input(
    snowflake_client,
    pipeline_name: str,
    index_name: str,
    start_ts: str,
    end_ts: str,
    database: str,
    schema: str,
    table: str,
) -> dict:
    """
    Checks if any existing records in Snowflake overlap with the given time window.

    Uses day-level filtering via query_window_start_day and query_window_end_day
    before checking actual timestamp overlaps.

    Args:
        snowflake_client (SnowflakeQueryClient): Active client instance.
        pipeline_name (str): Target pipeline name.
        index_name (str): Target index name.
        start_ts (str): New record's window start timestamp (ISO 8601).
        end_ts (str): New record's window end timestamp (ISO 8601).
        database (str): Snowflake database.
        schema (str): Snowflake schema.
        table (str): Snowflake table to search.

    Returns:
        dict:
            {
                "query_id": str,
                "data": pd.DataFrame (may be empty)
            }
    """
    logger = LogBlock(logger_name="data_pipeline", max_depth=6)
    key = "CHECK_OVERLAP_FOR_INPUT"
    logger.log_start(key)

    try:
        start_day = datetime.fromisoformat(start_ts).date().isoformat()
        end_day = datetime.fromisoformat(end_ts).date().isoformat()
    except Exception as e:
        logger.log_failure(key, f"Invalid timestamp format: {e}")
        raise ValueError(f"Invalid timestamp format: {e}")

    query = f"""
        WITH filtered_day_records AS (
            SELECT *
            FROM {table}
            WHERE query_window_start_day <= %(end_day)s
              AND query_window_end_day >= %(start_day)s
              AND pipeline_name = %(pipeline_name)s
              AND index_name = %(index_name)s
        )
        SELECT *
        FROM filtered_day_records
        WHERE query_window_start_ts < %(end_ts)s
          AND query_window_end_ts > %(start_ts)s
    """

    query_params = {
        "pipeline_name": pipeline_name,
        "index_name": index_name,
        "start_day": start_day,
        "end_day": end_day,
        "start_ts": start_ts,
        "end_ts": end_ts
    }

    try:
        result = snowflake_client.fetch_all_rows_as_dataframe(
            query=query,
            database=database,
            schema=schema,
            query_params=query_params
        )

        df = result["data"]
        query_id = result["query_id"]

        logger.log_complete(
            key=key,
            message=(
                f"STATUS: COMPLETED\n"
                f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}'\n"
                f"INPUT RANGE: [{start_ts} - {end_ts}]\n"
                f"RECORDS FOUND: {len(df)}\n"
                f"QUERY ID: {query_id}\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        return result

    except Exception as e:
        logger.log_failure(
            key=key,
            message=(
                f"STATUS: FAILED\n"
                f"FILTER: pipeline_name = '{pipeline_name}', index_name = '{index_name}'\n"
                f"INPUT RANGE: [{start_ts} - {end_ts}]\n"
                f"ERROR: {str(e)}\n"
                f"QUERY:\n{query.strip()}"
            )
        )
        raise e
