import os
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import yaml
from logging_setup import get_logger

logger = get_logger('pg')

class Database:
    def __init__(self):
        test_mode = os.getenv("PREDICTIVESITEOUTAGE_TEST_MODE", "false").lower() == "true"
        config_file_name = "db_config_dev.yaml" if test_mode else "db_config_prod.yaml"
        config_file_path = os.path.join(os.path.dirname(__file__), '..', 'configs', config_file_name)
        
        with open(config_file_path, 'r') as file:
            self.db_config = yaml.safe_load(file)
        
        # logger.info(f"Database configuration loaded from {config_file_name}.")

    def create_db_connection(self):
        connections = []
        connection_type = self.db_config.get("connection")

        if connection_type in ["alchemy", "both"]:
            try:
                engine = create_engine(
                    f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@"
                    f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
                )
                connections.append(engine.connect())
                logger.info("SQLAlchemy connection established.")
            except sqlalchemy.exc.SQLAlchemyError as e:
                logger.error(f"SQL Alchemy Connection Failed: {e}")
                return None

        if connection_type in ["pg", "both"]:
            try:
                connection_pg = psycopg2.connect(
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                    host=self.db_config["host"],
                    port=self.db_config["port"],
                    database=self.db_config["dbname"]
                )
                connection_pg.autocommit = True
                connections.append(connection_pg)
                logger.info("psycopg2 connection established.")
            except psycopg2.OperationalError as e:
                logger.error(f"psycopg2 Connection Failed: {e}")
                return None
        return connections

    def close_db_connection(self, connection):
        connection.close()
        logger.info("Database connection closed.")
