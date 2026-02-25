CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WORKMANAGEMENT_HISTORY
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WORKMANAGEMENT_HISTORY/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;