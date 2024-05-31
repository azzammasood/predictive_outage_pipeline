from logging_setup import get_logger

logger = get_logger('main')

temp_table = "tempoutpre"
set_work_mem_setquery = """ SET work_mem = '5GB' """
temp_file_limit_setquery = """ SET temp_file_limit = '15GB' """


def generate_predictive_query(utc_timestamp):
    predictive_query = f"""
    SELECT 
        msg.siteid, 
        msg.updatetime,
        msg.putotoutputcurr, 
        msg.acpowersourcestatus, 
        msg.loadtotalcurrent, 
        msg.batt1estdistime, 
        msg.batt2estdistime, 
        msg.batt3estdistime, 
        msg.batt4estdistime, 
        msg.batt5estdistime, 
        msg.batt6estdistime, 
        msg.batt7estdistime, 
        msg.batt8estdistime, 
        inv.generator1capacitykva, 
        inv.generator2capacitykva, 
        inv.generator3capacitykva, 
        inv.battery1brand,
        inv.battery2brand,
        inv.battery3brand, 
        inv.solar1capacitykw,
        inv.solar2capacitykw,
        inv.solar3capacitykw, 
        inv.transformer1capacitykva, 
        inv.transformer2capacitykva, 
        inv.transformer3capacitykva, 
        msg.hwcode, 
        msg.powerstate, 
        inv.fuelfactorhour, 
        msg.dcvoltage, 
        msg.blvdvoltage, 
        msg.llvd1voltage, 
        (SELECT AVG(x) FROM unnest(array[batt1soc, batt2soc, batt3soc, batt4soc, batt5soc, batt6soc, batt7soc, batt8soc]) AS x WHERE x BETWEEN 0 AND 100) AS battsoc, 
        inv.battery1capacityah,
        inv.battery2capacityah,
        inv.battery3capacityah
    FROM 
        public.siteinfra AS inv
    INNER JOIN 
        public.messagesrealtimemqtt_energy AS msg 
    ON 
        inv.siteid = msg.siteid
    WHERE 
        (msg.updatetime > {utc_timestamp} - 1800 AND msg.updatetime < {utc_timestamp}) 
        OR 
        (msg.updatetime > {utc_timestamp} - 86400 AND msg.updatetime < {utc_timestamp} - 86400 + 1500)
    """

    logger.info("Predictive query generated.")

    return predictive_query


pouring_Last_Date_Query = """ 
    SELECT s.siteid, s.date as pouringdate, s.fuelpouring
    FROM performance s
    JOIN (
        select siteid, max(date) as date
        from performance 
        where (date >= CURRENT_DATE - interval '30' day) and fuelpouring is not null and fuelpouring > 0
        group by siteid
    ) s2 
    ON s.siteid = s2.siteid AND s.date = s2.date
    order by siteid
    """


dg_query = """select siteid, dghr, siteloadkw, date from performance where (date >= CURRENT_DATE - interval '30' day) order by siteid, date"""


upsert_siteoutage_query = f"""
    INSERT INTO axin_temp(siteid, datetime, siteoutage, blvdoutage, dgfaulty, outagetime, currpowerstate)
    SELECT siteid, datetime, siteoutage, blvdoutage, dgfaulty, outagetime, currpowerstate 
    FROM {temp_table} 
    WHERE NOT EXISTS (
        SELECT 1 
        FROM axin_temp 
        WHERE {temp_table}.datetime = axin_temp.datetime 
        AND {temp_table}.siteid = axin_temp.siteid
    );
    UPDATE axin_temp
    SET siteoutage = {temp_table}.siteoutage, 
        blvdoutage = {temp_table}.blvdoutage, 
        dgfaulty = {temp_table}.dgfaulty, 
        outagetime = {temp_table}.outagetime, 
        currpowerstate = {temp_table}.currpowerstate
    FROM {temp_table}
    WHERE axin_temp.datetime = {temp_table}.datetime 
    AND axin_temp.siteid = {temp_table}.siteid
    """


upsert_fuel_data_query = f"""
    INSERT INTO performance_temp(siteid, date, remainingfuel)
    SELECT siteid, datetime, remainingfuel 
    FROM {temp_table} 
    WHERE NOT EXISTS (
        SELECT 1 
        FROM performance 
        WHERE {temp_table}.date = performance_temp.date 
        AND {temp_table}.siteid = performance_temp.siteid
    );
    UPDATE performance_temp
    SET remainingfuel = {temp_table}.remainingfuel
    FROM {temp_table}
    WHERE performance_temp.date = {temp_table}.date 
    AND performance_temp.siteid = {temp_table}.siteid
"""


messagesalert_query = f"""
SELECT siteid, displaypoint, opentime, closetime
FROM messagesalerthistory
WHERE opentime BETWEEN (EXTRACT(EPOCH FROM NOW()) - (30 * 24 * 60 * 60)) AND EXTRACT(EPOCH FROM NOW());
"""

