CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLINTERFACE_NOTIFICATIONSTAGING
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLINTERFACE_NOTIFICATIONSTAGING/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;