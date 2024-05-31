from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lit, first, last, sum, rank, concat_ws, collect_list, expr, greatest, to_timestamp, to_date
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from logging_setup import get_logger

logger = get_logger('predictive_outage')


def predictive_outage(data, pouring_l_day, performance_Data_Df, current_time):
    pouring_l_day = pouring_l_day.withColumnRenamed("siteid", "siteid1")
    logger.info("Fuel pouring data renamed and prepared for merging.")

    # Merge performance and fuel pouring data, fill nulls in performance df with 0
    merge_performance_pouring = performance_Data_Df.join(pouring_l_day, pouring_l_day.siteid1 == performance_Data_Df.siteid, "left").drop("siteid1").na.fill(value=0, subset=["dghr"]).withColumn("dghr", when(col("dghr") == "NULL", 0).otherwise(col("dghr")))
    logger.info("Merged performance and fuel pouring data.")

    # Filter merged data and get rows where date greater than pouring date
    merge_performance_pouring = merge_performance_pouring.filter(col("date") >= col("pouringdate"))
    logger.info("Filtered merged data based on pouring date.")

    # From merged data, for each site, sum dghr, get latest fuel pouring, get max site load kw
    sum_df_performance_dg = merge_performance_pouring.groupBy("siteid").agg(
        sum(col("dghr")).alias("dghr"),
        last(col("fuelpouring")).alias("fuelpouring"),
        max(col("siteloadkw")).alias("siteloadkw")
    ).withColumnRenamed("siteid", "siteid1")
    logger.info("Aggregated performance and fuel pouring data.")

    # Filter realtime data, get data for the past 30 minutes
    data_df = data.filter(data.updatetime >= current_time - 1800)
    logger.info("Filtered real-time data for the past 30 minutes.")

    # Rank rows in realtime data based on updatetime to get the two most recent rows
    window = Window.partitionBy(data_df['siteid']).orderBy(data_df['updatetime'].desc())
    data_df2 = data_df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 2)
    logger.info("Ranked real-time data based on updatetime to get the two most recent rows.")

    data_Cols = data_df2.columns
    all_Cols = [col for col in data_Cols if (col != "hwcode" and col != 'siteid' and col != 'putotoutputcurr')]
    data_df4 = data_df2.groupBy("siteid").agg(
        concat_ws("+", collect_list("hwcode")).alias("hwcode"), 
        concat_ws("+", collect_list("putotoutputcurr")).alias("putotoutputcurr"), 
        *[first(i).alias(i) for i in data_df2.select(all_Cols).columns]
    )
    logger.info("Grouped and aggregated real-time data.")

    # Filter realtime data to get rows where rectifier-2 is not used twice
    data_df5 = data_df4.filter((col("hwcode") != "rectifier-2+rectifier-2"))
    logger.info("Filtered real-time data to remove rows where rectifier-2 is used twice.")

    data_df6 = data_df5.withColumn("subtext", lit("+"))
    data_df7 = data_df6.withColumn('position', F.expr('position(subtext, putotoutputcurr) - 1'))

    data_df8 = data_df7.withColumn("putotoutputcurr", when(
        data_df7.hwcode == "rectifier-1+rectifier-1", expr("substring(putotoutputcurr, 1, position)").alias("substring_result")
    ).when(
        data_df7.hwcode == "rectifier-1+rectifier-2", expr("substring(putotoutputcurr, 1, position)").alias("substring_result")
    ).when(
        data_df7.hwcode == "rectifier-2+rectifier-1", expr("substring(putotoutputcurr, position, -1)").alias("substring_result")
    ))
    logger.info("Transformed putotoutputcurr column based on hwcode.")

    data_df9 = data_df8.withColumn("hwcode", lit("rectifier-1"))
    data_df10 = data_df9.drop("rank", "subtext", "position")
    logger.info("Transformed hwcode column.")

    data_df11 = data_df10.withColumn("power_status_category", when(
        ((col("tfcapacitykva").isNotNull()) & (col("tfcapacitykva") != 0)) & (col("solarkw").isNotNull()) & (col("batterytechnology").isNotNull()) & ((col("dgkva").isNotNull()) & (col("dgkva") != 0)), 1
    ).when(
        (col("solarkw").isNotNull()) & (col("batterytechnology").isNotNull()) & ((col("dgkva").isNotNull()) & (col("dgkva") != 0)), 2
    ).when(
        (col("tfcapacitykva").isNotNull()) & (col("batterytechnology").isNotNull()) & ((col("dgkva").isNotNull()) & (col("dgkva") != 0)), 3
    ).when(
        (col("tfcapacitykva").isNotNull()) & (col("solarkw").isNotNull()) & (col("batterytechnology").isNotNull()), 4
    ).when(
        (col("batterytechnology").isNotNull()) & ((col("dgkva").isNotNull()) & (col("dgkva") != 0)), 5
    ).when(
        (col("tfcapacitykva").isNotNull()) & (col("batterytechnology").isNotNull()), 6
    ).when(
        ((col("dgkva").isNotNull()) & (col("dgkva") != 0)), 7
    ).otherwise(lit(8))).withColumnRenamed("powerstate", "currpowerstate")
    logger.info("Categorized power status.")

    past_data_df = data.filter(data.updatetime < current_time - 1800).drop("updatetime")
    powerstate_list = ["-", "DG", "DG-batt", "batt", "solar", "solar-batt", "solar-DG", "wind", "wind-mains", "outage", "solar-mains", "mains-batt"]
    past_data_df1 = past_data_df.withColumn("powerstate", when(col("powerstate").isin(powerstate_list), None).otherwise(col("powerstate")))
    past_data_df2 = past_data_df1.groupBy("siteid").agg(last(col("powerstate"), True).alias("powerstate")).withColumnRenamed("siteid", "siteid2")
    logger.info("Processed past data.")

    merge_df = data_df11.join(past_data_df2, past_data_df2["siteid2"] == data_df11["siteid"], "left").drop("siteid2")
    merge_df = merge_df.withColumn("putotoutputcurr", merge_df.putotoutputcurr.cast(DoubleType()))
    merge_df1 = merge_df.withColumn("battestdistime", greatest(col("batt1estdistime"), col("batt2estdistime"), col("batt3estdistime"), col("batt4estdistime"), col("batt5estdistime"), col("batt6estdistime"), col("batt7estdistime"), col("batt8estdistime")))
    merge_df1 = merge_df1.drop("batt1estdistime","batt2estdistime", "batt3estdistime", "batt4estdistime", "batt5estdistime","batt6estdistime", "batt7estdistime", "batt8estdistime")
    merge_df1 = merge_df1.na.fill(value=0, subset=["putotoutputcurr", "loadtotalcurrent", "battestdistime", "acpowersourcestatus", "battsoc"])
    logger.info("Merged and processed real-time and past data.")

    merge_df2 = merge_df1.join(sum_df_performance_dg, merge_df1.siteid == sum_df_performance_dg.siteid1, "left").drop("siteid1")
    merge_df2 = merge_df2.withColumn("fuel_comsumtion", col("fuelfactorhour") * col("dghr")).withColumn("remainingfuel", col("fuelpouring") - col("fuel_comsumtion"))
    merge_df3 = merge_df2.withColumn("siteoutage", lit("False"))
    merge_df3 = merge_df3.withColumn("siteoutage", when(
        (col("power_status_category") == 1) & (((~ col("acpowersourcestatus").isin([1,4,5,6,7])) & (col("putotoutputcurr") < col("loadtotalcurrent")) & 
        ((col("powerstate").isNull()) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90)))) & 
        ((col("remainingfuel") <= 0) | ((col("siteloadkw") / 2) > col("remainingfuel") *6) | (col("dcvoltage") <= col("llvd1voltage")))) | 
        ((col("dcvoltage") < (col("blvdvoltage") + 2)) & (col("dcvoltage") > 0) & 
        (col("blvdvoltage") > 0) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))) & 
        (~ col("acpowersourcestatus").isin([1,4,5,6,7])))), "True").otherwise(col("siteoutage")))
    logger.info("Processed site outage status for power status category 1.")

    merge_df3 = merge_df3.withColumn("siteoutage", when(
        (col("power_status_category") == 2) & (((col("putotoutputcurr") < col("loadtotalcurrent")) & 
        ((col("powerstate").isNull()) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90)))) & 
        ((col("remainingfuel") <= 0) | ((col("siteloadkw") / 2) > col("remainingfuel") *6) | (col("dcvoltage") <= col("llvd1voltage")))) | 
        ((col("dcvoltage") < (col("blvdvoltage") + 2)) & (col("dcvoltage") > 0) & 
        (col("blvdvoltage") > 0) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90)))), "True").otherwise(col("siteoutage"))))
    logger.info("Processed site outage status for power status category 2.")

    merge_df3 = merge_df3.withColumn("siteoutage", when(
        (col("power_status_category") == 3) & (((~ col("acpowersourcestatus").isin([1,4,5,6,7])) & 
        ((col("powerstate").isNull()) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90)))) & 
        ((col("remainingfuel") <= 0) | ((col("siteloadkw") / 2) > col("remainingfuel") *6) | (col("dcvoltage") <= col("llvd1voltage")))) | 
        ((col("dcvoltage") < (col("blvdvoltage") + 2)) & (col("dcvoltage") > 0) & 
        (col("blvdvoltage") > 0) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))) & 
        (~ col("acpowersourcestatus").isin([1,4,5,6,7])))), "True").otherwise(col("siteoutage")))
    logger.info("Processed site outage status for power status category 3.")

    merge_df3 = merge_df3.withColumn("siteoutage", when(
        (col("power_status_category") == 4) & (((~ col("acpowersourcestatus").isin([1,4,5,6,7])) & 
        (col("putotoutputcurr") < col("loadtotalcurrent")) & 
        ((col("powerstate").isNull()) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))))) | 
        (((col("dcvoltage") < (col("blvdvoltage") + 2))) & (col("dcvoltage") > 0) & 
        (col("blvdvoltage") > 0) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))) & 
        (~ col("acpowersourcestatus").isin([1,4,5,6,7])))), "True").otherwise(col("siteoutage")))
    logger.info("Processed site outage status for power status category 4.")

    merge_df3 = merge_df3.withColumn("siteoutage", when(
        (col("power_status_category") == 5) & (((((col("powerstate").isNull()) & 
        (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90)))) & 
        ((col("remainingfuel") <= 0) | ((col("siteloadkw") / 2) > col("remainingfuel") *6) | (col("dcvoltage") <= col("llvd1voltage")))) | 
        (((col("dcvoltage") < (col("blvdvoltage") + 2))) & (col("dcvoltage") > 0) & 
        (col("blvdvoltage") > 0) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))))), "True").otherwise(col("siteoutage"))))
    logger.info("Processed site outage status for power status category 5.")

    merge_df3 = merge_df3.withColumn("siteoutage", when(
        (col("power_status_category") == 6) & (((~ col("acpowersourcestatus").isin([1,4,5,6,7])) & 
        ((col("powerstate").isNull()) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))))) | 
        (((col("dcvoltage") < (col("blvdvoltage") + 2))) & (col("dcvoltage") > 0) & 
        (col("blvdvoltage") > 0) & (((col("battestdistime") > 0) & (col("battestdistime") < 90)) | 
        (((col("battsoc") > 0) & (col("battsoc") < 50)) & (col("battestdistime") < 0) & 
        (((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent")) < 90))) & 
        (~ col("acpowersourcestatus").isin([1,4,5,6,7])))), "True").otherwise(col("siteoutage")))
    logger.info("Processed site outage status for power status category 6.")

    merge_df3 = merge_df3.withColumn("siteoutage", when((col("power_status_category") == 7), "pass").otherwise(col("siteoutage")))
    merge_df3 = merge_df3.withColumn("siteoutage", when((col("power_status_category") == 8), "pass").otherwise(col("siteoutage")))
    logger.info("Processed site outage status for power status categories 7 and 8.")

    merge_df3 = merge_df3.withColumn("blvdoutage", when((col("dcvoltage") <= col("blvdvoltage")) & (~ col("acpowersourcestatus").isin([1,4,5,6,7])), "True").otherwise("False"))
    logger.info("Processed blvd outage status.")

    merge_df3 = merge_df3.withColumn("dgfaulty", when(col("dcvoltage") < col("llvd1voltage"), "True").otherwise("False"))
    logger.info("Processed DG faulty status.")

    merge_df3 = merge_df3.withColumn("outagetime", when(
        ((col("siteoutage") == "True") | (col("blvdoutage") == "True")) & (col("blvdoutage") == "True"), 0
    ).when(
        (col("siteoutage") == "True") & ((col("battestdistime") < 0) & ((col("battsoc") < 50)) & (col("dgfaulty") == "True")), 0
    ).when(
        (col("siteoutage") == "True") & ((col("battestdistime") < 0) & (col("battsoc") < 50) & 
        ((col("remainingfuel") < 0) | (col("remainingfuel").isNull()))), 
        ((col("batteryratingah") * col("battsoc")) / col("loadtotalcurrent"))
    ).when(
        (col("siteoutage") == "True") & ((col("battestdistime") < 0) & (col("remainingfuel") > 0) & (col("battsoc") < 50)), 0
    ).when(
        (col("siteoutage") == "True") & ((col("battestdistime") < 0) & (col("remainingfuel") > 0) & 
        (col("dgfaulty") == "True") & (col("battsoc") < 50)), 
        ((col("siteloadkw") * 1.5) / 10.6) * col("remainingfuel")
    ).otherwise(col("battestdistime")))
    logger.info("Processed outage time.")

    merge_df3 = merge_df3.withColumn("datetime", lit(current_time)).withColumn("datetime", to_timestamp(col("datetime")).alias("datetime")).withColumn("date", to_date(col("datetime")))
    merge_df3 = merge_df3.repartition(8)
    logger.info("Finished Spark RDD transformations.")
    
    return merge_df3
