from base import Base
import datetime as dt
import sys
from pg import Database
import queries as q
from predictive_outage import predictive_outage
import os
from func import get_utc_timestamp
from logging_setup import get_logger

logger = get_logger('main')

def main():
    test_mode = os.getenv("PREDICTIVESITEOUTAGE_TEST_MODE", "false").lower() == "true"
    logger.info(f"Running Predictive Site Outage pipeline in test={test_mode} mode")

    rounded_utc_timestamp = get_utc_timestamp()

    spark = Base.spark_Session("predictive_outage")

    predictive_query = q.generate_predictive_query(rounded_utc_timestamp)

    data_Df = Base.read_Postgres(spark, predictive_query, query_info="realtime and inventory")

    pouring_lday = Base.read_Postgres(spark, q.pouring_Last_Date_Query, query_info="fuel pouring")

    performance_Data_Df = Base.read_Postgres(spark, q.dg_query, query_info="performance")

    data = predictive_outage(data_Df, pouring_lday, performance_Data_Df, rounded_utc_timestamp)
    logger.info("Predictive outage data generated.")

    db = Database()

    if data:
        # Determine the path dynamically
        results_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'results', 'predictive_outage_results')
        os.makedirs(results_path, exist_ok=True)

        data.coalesce(1).write.mode('overwrite').csv(results_path, header=True)
        logger.info(f"Results written to CSV at {results_path}")

        data.printSchema()
        data.show(10, truncate=False)

        Base.write_Postgres(data=data, table=q.temp_table)
        logger.info("Data written to PostgreSQL.")

        connections = db.create_db_connection()
        if not connections:
            logger.error("Database connection failed.")
            sys.exit(1)

        conn = connections[1]
        cursor = conn.cursor()

        cursor.execute(q.set_work_mem_setquery)
        conn.commit()
        cursor.execute(q.temp_file_limit_setquery)
        conn.commit()

        cursor.execute(q.upsert_siteoutage_query)
        conn.commit()
        logger.info(f"Moved data from {q.temp_table} table to axin_temp table")

        cursor.execute(q.set_work_mem_shquery)
        set_work_mem_df = cursor.fetchall()[0][0]
        logger.info(f"Set work memory: {set_work_mem_df}")

        cursor.execute(q.temp_file_limit_shquery)
        temp_file_limit_df = cursor.fetchall()[0][0]
        logger.info(f"Temp file limit: {temp_file_limit_df}")

        if dt.replace(hour=0, minute=0, second=0, microsecond=0) <= dt < dt.replace(hour=0, minute=5, second=0, microsecond=0):
            cursor.execute(q.upsert_fuel_data_query)
            conn.commit()
            logger.info(f"Moved data from {q.temp_table} table to performance table for remaining fuel")
        
        conn.close()

if __name__ == "__main__":
    main()
