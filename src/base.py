import os
import yaml
from pyspark.sql import SparkSession
import psycopg2
from logging_setup import get_logger

logger = get_logger('base')


class Base:
    @staticmethod
    def spark_Session(session_name):
        spark_session = SparkSession\
                .builder\
                .appName(f"{session_name}")\
                .config("spark.jars", "/home/azzam/spark/jars/postgresql-42.7.2.jar")\
                .config("spark.driver.extraClassPath", "/home/azzam/spark/jars/postgresql-42.7.2.jar")\
                .getOrCreate()
        spark_session.sparkContext.setLogLevel('WARN')
        logger.info("Spark session initialized.")
        return spark_session

    @staticmethod
    def read_db_config():
        test_mode = os.getenv("PREDICTIVESITEOUTAGE_TEST_MODE", "false").lower() == "true"
        config_file = "db_config_dev.yaml" if test_mode else "db_config_prod.yaml"
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', config_file)

        with open(config_path, 'r') as file:
            db_config = yaml.safe_load(file)

        # logger.info(f"Database configuration loaded from {config_file}.")
        return db_config

    @staticmethod
    def read_Postgres(spark, query, query_info):
        db_config = Base.read_db_config()
        url = db_config['url']
        user = db_config['user']
        password = db_config['password']

        logger.info(f"Reading {query_info} data from PostgreSQL with URL: {url}, User: {user}")
        data_df = spark.read\
                        .format("jdbc")\
                        .option("url", url)\
                        .option("dbtable", f"({query}) as subquery")\
                        .option("user", user)\
                        .option("password", password)\
                        .option("driver", "org.postgresql.Driver")\
                        .load()
        logger.info(f"{query_info} data read successfully from PostgreSQL.")
        return data_df

    @staticmethod
    def write_Postgres(data, table):
        db_config = Base.read_db_config()
        url = db_config['url']
        user = db_config['user']
        password = db_config['password']

        logger.info(f"Writing data to PostgreSQL with URL: {url}, User: {user}, Table: {table}")
        data.write\
            .format("jdbc")\
            .option("url", url)\
            .option("dbtable", f"public.{table}")\
            .option("user", user)\
            .option("password", password)\
            .option("driver", "org.postgresql.Driver")\
            .mode("overwrite")\
            .save()
        logger.info("Data inserted successfully into PostgreSQL.")
        return True

    @staticmethod
    def move_Temp_Data(query, database, user, password, host, port):
        db_conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        dbc_select = db_conn.cursor()
        dbc_select.execute(query)
        db_conn.commit()
        logger.info("Data moved from temp table to main table.")
        db_conn.close()

    @staticmethod
    def truncate_Temp(query, database, user, password, host, port):
        db_conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        dbc_select = db_conn.cursor()
        dbc_select.execute(query)
        db_conn.commit()
        logger.info("Data truncated from temp table.")
        db_conn.close()

    @staticmethod
    def drop_Temp(query, database, user, password, host, port):
        db_conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        dbc_select = db_conn.cursor()
        dbc_select.execute(query)
        db_conn.commit()
        logger.info("Temp table dropped.")
        db_conn.close()

    @staticmethod
    def missingFillBinsDf(_spark, data_df, st, en):
        from pyspark.sql.functions import col, row_number, monotonically_increasing_id, floor, coalesce

        n = 300
        start_pd = _spark.range(st, en-300+1, n).withColumnRenamed("id", "start_bin")
        end_pd = _spark.range(st+300, en+1, n).withColumnRenamed("id", "end_bin")

        df1 = start_pd.withColumn("row_id", monotonically_increasing_id())
        df2 = end_pd.withColumn("row_id", monotonically_increasing_id())
        joined_st_en = df1.join(df2, on="row_id", how="outer")
        joined_st_en = joined_st_en.withColumn("hour", floor(col("row_id") / 12) + 1)
        all_sites = data_df.select('siteid').distinct()

        bins_df = all_sites.crossJoin(joined_st_en).withColumnRenamed("siteid", "siteid2")

        cond = [data_df.siteid == bins_df.siteid2,
                data_df.updatetime >= bins_df.start_bin,
                data_df.updatetime < bins_df.end_bin]
        merge_df = bins_df.join(data_df, cond, 'left')
        merge_df = merge_df.withColumn("siteid", coalesce(col("siteid"), col("siteid2"))).drop("siteid2")
        merge_df = merge_df.repartition(8)
        logger.info("Filled missing bins in data.")
        return merge_df

    @staticmethod
    def fillBinsDf(_spark, data_df, st, en):
        from pyspark.sql.functions import monotonically_increasing_id, floor

        n = 300
        start_pd = _spark.range(st, en-300+1, n).withColumnRenamed("id", "start_bin")
        end_pd = _spark.range(st+300, en+1, n).withColumnRenamed("id", "end_bin")

        df1 = start_pd.withColumn("row_id", monotonically_increasing_id())
        df2 = end_pd.withColumn("row_id", monotonically_increasing_id())
        joined_st_en = df1.join(df2, on="row_id", how="outer")
        joined_st_en = joined_st_en.withColumn("hour", floor(col("row_id") / 12) + 1)
        all_sites = data_df.select('siteid').distinct()

        bins_df = all_sites.crossJoin(joined_st_en).select("siteid", "start_bin", "end_bin", "hour")
        bins_df = bins_df.repartition(8)
        logger.info("Filled bins in data.")
        return bins_df

    @staticmethod
    def calcMode(arr):
        from collections import Counter
        c = Counter(filter(lambda x: x is not None and x != "-", arr))
        if c:
            mode_value = c.most_common()[0][0]
        else:
            mode_value = None
        logger.info(f"Calculated mode: {mode_value}")
        return mode_value
