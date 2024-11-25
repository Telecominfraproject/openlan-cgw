import psycopg2
from psycopg2 import OperationalError, sql
from typing import List, Tuple

class PostgreSQLClient:
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize the PostgreSQL client with the connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """Connect to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection successful")
        except OperationalError as e:
            print(f"Error: Unable to connect to the database. {e}")

    def execute_query(self, query: str, params=None):
        """Execute a single query (SELECT, INSERT, UPDATE, DELETE, etc.)."""
        if not self.cursor:
            print("Error: No database connection established.")
            return None
        
        try:
            # Use sql.SQL for parameterized queries to avoid SQL injection
            if params:
                self.cursor.execute(sql.SQL(query), params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully")
        except Exception as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
            return None

    def fetchone(self):
        """Fetch one row from the last executed query (used with SELECT)."""
        result = None

        if self.cursor:
            try: 
                result = self.cursor.fetchone()
            except Exception as e:
                print(f"Error executing fetchone: {e}")
        else:
            print("Error: No database connection or query executed.")
        
        return result

    def fetchall(self):
        """Fetch all rows from the last executed query (used with SELECT)."""
        result = None

        if self.cursor:
            try:
                result = self.cursor.fetchall()
            except Exception as e:
                print(f"Error executing fetchone: {e}")
        else:
            print("Error: No database connection or query executed.")
        
        return result

    def get_infrastructure_group(self, group_id: int) -> Tuple[int, int, int]:
        """Fetch group record by group id."""
        group_info = tuple()

        self.execute_query(f"select * from infrastructure_groups WHERE id = {group_id};")
        group_info = self.fetchone()

        return group_info

    def get_all_infrastructure_groups(self) -> List[Tuple[int, int, int]]:
        """Fetch group record by group id."""
        group_list = list()

        self.execute_query(f"select * from infrastructure_groups;")
        group_list = self.fetchall()

        return group_list

    def get_infra(self, mac: str) -> Tuple[str, int]:
        """Fetch group record by infra mac."""
        infra_info = None

        self.execute_query(f"select * from infras WHERE mac = \'{mac}\';")
        infra_info = self.fetchone()

        # change mac format from "XX:XX:XX:XX:XX:XX" to "XX-XX-XX-XX-XX-XX"
        if infra_info:
            temp_infra = list(infra_info)
            temp_infra[0] = temp_infra[0].replace(":", "-", 5)
            infra_info = tuple(temp_infra)

        return infra_info

    def get_infras_by_group_id(self, group_id) -> List[Tuple[str, int]]:
        """Fetch group record by infra mac."""
        infras_info = None

        self.execute_query(f"select * from infras WHERE infra_group_id = \'{group_id}\';")
        infras_info = self.fetchall()

        return infras_info

    def disconnect(self):
        """Close the cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed.")
