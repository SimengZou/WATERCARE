CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLINTERFACE_CONNECTIONSSTAGING
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLINTERFACE_CONNECTIONSSTAGING/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;