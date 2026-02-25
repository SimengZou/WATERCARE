CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WORKMANAGEMENT_COSTXTRA
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WORKMANAGEMENT_COSTXTRA/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;