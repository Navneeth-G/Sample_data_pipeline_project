import snowflake.connector
from typing import Optional, Any
import pandas as pd


class SnowflakeQueryClient:
    """
    A reusable and extensible client for executing SQL queries in Snowflake.

    ⚙ Features:
    - Establishes a connection to Snowflake with reusable credentials
    - Executes queries that return:
        - A single scalar value (e.g., COUNT, MAX)
        - Full result tables as a Pandas DataFrame or list of tuples
    - Supports parameterized queries via Python dict input
    - Returns Snowflake query ID with each query for traceability

     Usage Example:

        client = SnowflakeQueryClient(
            account='abc-xy12345.ap-south-1',
            user='my_user',
            password='my_pass',
            role='SYSADMIN',
            warehouse='COMPUTE_WH',
            database='MY_DB',
            schema='PUBLIC'
        )

        # Scalar query
        result = client.execute_scalar_query(
            query="SELECT COUNT(*) FROM orders WHERE status = %(status)s",
            database="MY_DB",
            schema="PUBLIC",
            query_params={"status": "shipped"}
        )
        print(result["data"], result["query_id"])

        # Table query
        df_result = client.fetch_all_rows_as_dataframe(
            query="SELECT * FROM orders WHERE created_at > %(date)s",
            database="MY_DB",
            schema="PUBLIC",
            query_params={"date": "2023-01-01"}
        )
        print(df_result["data"].head())

    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str
    ):
        """
        Initializes connection credentials for Snowflake.

        🔐 All parameters are required to establish the connection.

        Args:
            account (str): Snowflake account identifier (e.g., 'xy12345.ap-south-1').
            user (str): Snowflake user login.
            password (str): User's password.
            role (str): Snowflake role (e.g., 'SYSADMIN').
            warehouse (str): Virtual warehouse to execute queries.
            database (str): Default database (can be overridden per query).
            schema (str): Default schema (can be overridden per query).
        """
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection: Optional[Any] = None

    def _create_snowflake_connection(self) -> Any:
        """
        Internal method to create a new connection to Snowflake.

        Returns:
            A valid connection object.

        Raises:
            ConnectionError: If connection fails (e.g., invalid credentials).
        """
        try:
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                role=self.role,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            return conn
        except Exception as error:
            raise ConnectionError(f"Failed to connect to Snowflake: {error}")

    def get_active_connection(self) -> Any:
        """
        Gets or reuses an open Snowflake connection.

        Returns:
            Active connection object.
        """
        if self.connection is None or self.connection.is_closed():
            self.connection = self._create_snowflake_connection()
        return self.connection

    def execute_scalar_query(
        self,
        query: str,
        database: str,
        schema: str,
        query_params: Optional[dict] = None
    ) -> dict:
        """
        Executes a query that returns a single scalar value (e.g., COUNT, MAX, SUM).

        Use this for:
        - Aggregates
        - Checks (e.g., COUNT(*) WHERE ...)

        Args:
            query (str): SQL query string with optional named placeholders (e.g., %(status)s).
            database (str): Database to use for this query.
            schema (str): Schema to use for this query.
            query_params (dict, optional): Key-value pairs to bind to placeholders.

        Returns:
            dict:
                {
                    "query_id": str,  # Snowflake-assigned query ID
                    "data": scalar result or None
                }

        Raises:
            RuntimeError: If execution fails or query is invalid.
        """
        conn = self.get_active_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
                cursor.execute(query, query_params or {})
                query_id = cursor.sfqid
                result = cursor.fetchone()
                return {
                    "query_id": query_id,
                    "data": result[0] if result else None
                }
        except Exception as error:
            raise RuntimeError(f"Failed to execute scalar query: {error}")

    def fetch_all_rows_as_dataframe(
        self,
        query: str,
        database: str,
        schema: str,
        query_params: Optional[dict] = None
    ) -> dict:
        """
        Executes a query and returns the result as a Pandas DataFrame.

        Use this for:
        - Reading full tables or filtered views into memory
        - Exporting to CSV/parquet
        - Applying pandas operations

        Args:
            query (str): SQL query with optional placeholders (e.g., %(start_date)s).
            database (str): Target database.
            schema (str): Target schema.
            query_params (dict, optional): Key-value dict to bind in query.

        Returns:
            dict:
                {
                    "query_id": str,
                    "data": pd.DataFrame  # Can be empty
                }

        Raises:
            RuntimeError: If query execution or conversion fails.
        """
        conn = self.get_active_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
                cursor.execute(query, query_params or {})
                query_id = cursor.sfqid
                df = cursor.fetch_pandas_all()
                return {
                    "query_id": query_id,
                    "data": df
                }
        except Exception as error:
            raise RuntimeError(f"Failed to fetch rows as DataFrame: {error}")

    def fetch_all_rows_as_tuples(
        self,
        query: str,
        database: str,
        schema: str,
        query_params: Optional[dict] = None
    ) -> dict:
        """
        Executes a query and returns the result as a list of tuples.

        Use this if:
        - You don’t need a DataFrame
        - You want native Python structures for iteration or JSON export

        Args:
            query (str): SQL query with optional placeholders (e.g., %(user_id)s).
            database (str): Target database.
            schema (str): Target schema.
            query_params (dict, optional): Values to substitute in the query.

        Returns:
            dict:
                {
                    "query_id": str,
                    "data": list[tuple]  # Can be empty
                }

        Raises:
            RuntimeError: On query failure or connection error.
        """
        conn = self.get_active_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")
                cursor.execute(query, query_params or {})
                query_id = cursor.sfqid
                rows = cursor.fetchall()
                return {
                    "query_id": query_id,
                    "data": rows
                }
        except Exception as error:
            raise RuntimeError(f"Failed to fetch rows as tuples: {error}")

	def execute_dml_query(
		self,
		query: str,
		database: str,
		schema: str,
		query_params: Optional[dict] = None
	) -> dict:
		"""
		Executes a DML query (INSERT, UPDATE, DELETE) and returns query ID and affected row count.

		Use this method when your query modifies data in a table and you want to know how many rows were impacted.

		Args:
			query (str): SQL DML query with optional %(key)s-style placeholders.
			database (str): Target database to switch into.
			schema (str): Target schema to switch into.
			query_params (dict, optional): Parameters to bind in the query.

		Returns:
			dict: {
				"query_id": str,
				"rows_affected": int
			}

		Raises:
			RuntimeError: If query execution fails.
		"""
		conn = self.get_active_connection()
		try:
			with conn.cursor() as cursor:
				cursor.execute(f"USE DATABASE {database}")
				cursor.execute(f"USE SCHEMA {schema}")
				cursor.execute(query, query_params or {})
				query_id = cursor.sfqid
				rows_affected = cursor.rowcount
				return {
					"query_id": query_id,
					"rows_affected": rows_affected
				}
		except Exception as error:
			raise RuntimeError(f"Failed to execute DML query: {error}")

	def execute_control_command(
		self,
		query: str,
		database: str,
		schema: str,
		query_params: Optional[dict] = None
	) -> dict:
		"""
		Executes control commands such as CALL, ALTER TASK/PROCEDURE without expecting row results.

		Use this method to:
		- Trigger stored procedures
		- Resume, suspend, or alter Snowflake tasks
		- Execute system control logic

		Args:
			query (str): SQL command with optional %(key)s-style placeholders.
			database (str): Target database to switch into.
			schema (str): Target schema to switch into.
			query_params (dict, optional): Parameters to bind in the query.

		Returns:
			dict: {
				"query_id": str
			}

		Raises:
			RuntimeError: If query execution fails.
		"""
		conn = self.get_active_connection()
		try:
			with conn.cursor() as cursor:
				cursor.execute(f"USE DATABASE {database}")
				cursor.execute(f"USE SCHEMA {schema}")
				cursor.execute(query, query_params or {})
				query_id = cursor.sfqid
				return {
					"query_id": query_id
				}
		except Exception as error:
			raise RuntimeError(f"Failed to execute control command: {error}")
