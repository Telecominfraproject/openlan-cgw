import redis
import json


class RedisClient:
    def __init__(self, host: str, port: int):
        """Initialize the Redis client with the connection parameters."""
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        """Connect to the Redis database."""
        try:
            # Establish connection to Redis server
            self.connection = redis.StrictRedis(
                host=self.host, port=self.port,
                db=0, decode_responses=True, socket_timeout=5.0,
                socket_connect_timeout=2.0)
            # Check if the connection is successful
            self.connection.ping()
            print(f"Connected to Redis server at {self.host}:{self.port}")
        except redis.ConnectionError as e:
            print(f"Unable to connect to Redis: {e}")
            self.connection = None

    def select_db(self, db_id: int):
        """Selects a different database using the SELECT command."""
        if self.connection:
            try:
                self.connection.select(db_id)
                print(f"Switched to database {db_id}")
            except redis.RedisError as e:
                print(f"Error selecting database {db_id}: {e}")
        else:
            print("Redis client not connected.")

    def get_infrastructure_group(self, group_id: int) -> dict:
        return self.connection.hgetall(f"group_id_{group_id}")

    def get_shard(self, shard_id: int) -> dict:
        return self.connection.hgetall(f"shard_id_{shard_id}")

    def get_infra(self, shard_id: int, mac: str) -> dict:
        infra = None
        self.select_db(1)

        infra = self.connection.get(f"shard_id_{shard_id}|{mac}")
        if infra:
            infra = json.loads(infra)

        self.select_db(0)

        return infra

    def get(self, key: str) -> str:
        """Gets the value of a key."""
        result = None

        if self.connection:
            try:
                result = self.connection.get(key)
            except redis.RedisError as e:
                print(f"Error getting {key}: {e}")
        else:
            print("Redis client not connected.")

        return result

    def hgetall(self, hash_name: str) -> dict:
        """Gets all fields and values from a Redis hash."""
        result = None

        if self.connection:
            try:
                result = self.connection.hgetall(hash_name)
            except redis.RedisError as e:
                print(f"Error getting all fields from hash {hash_name}: {e}")
        else:
            print("Redis client not connected.")

        return result

    def exists(self, key: str) -> bool:
        """Checks if a key exists in Redis."""
        if self.connection:
            try:
                return self.connection.exists(key)
            except redis.RedisError as e:
                print(f"Error checking existence of {key}: {e}")
        else:
            print("Redis client not connected.")
        return False

    def disconnect(self):
        """Closes the Redis connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed.")
        else:
            print("No active Redis client connection to close.")
