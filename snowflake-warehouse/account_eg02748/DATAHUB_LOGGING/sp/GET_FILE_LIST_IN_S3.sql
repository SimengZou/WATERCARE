CREATE OR REPLACE PROCEDURE DATAHUB_LOGGING.GET_FILE_LIST_IN_S3(
    tablename varchar,
    appname varchar
)
returns table ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
handler='get_file_list_in_s3'
AS $$
from snowflake.snowpark.functions import col

def get_file_list_in_s3(session, tablename, appname):
    session.sql('ls @DATAHUB_INTEGRATION.STAGE_' + tablename).collect()

    return session.sql('''
                SELECT CASE 
                        WHEN upper(\''''+ appname + '''\') = 'LN' or 
                             upper(\''''+ appname + '''\') = 'DEPM' 
                        THEN replace("name", lower('s3://wsl-${buildvar.env_lower}-deinf-''' + appname + '''-landing-s3/'), '')
                        WHEN upper(\''''+ appname + '''\') = 'EAM' or 
                             upper(\''''+ appname + '''\') = 'IPS'
                        THEN replace("name", lower('s3://${buildvar.infors3bucket}-datalake-inforapptables/'), '')
                        ELSE ''
                    END filename
                FROM table(result_scan(last_query_id()))
                ORDER BY 1
            ''')
$$;