import psycopg2

from sqlalchemy import create_engine


class DatabaseConnection:
    def __init__(self, db_config):
        self.db_config = db_config
        self.engine = None
        self.pg_connection = None

    def connect(self):
        try:
            self.engine = create_engine(
                f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
            )
            self.pg_connection = psycopg2.connect(
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["dbname"]
            )
            self.pg_connection.autocommit = True
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            raise

    def close(self):
        if self.engine:
            self.engine.dispose()
        if self.pg_connection:
            self.pg_connection.close()