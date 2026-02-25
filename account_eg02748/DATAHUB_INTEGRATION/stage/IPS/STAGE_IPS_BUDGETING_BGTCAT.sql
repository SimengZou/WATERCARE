CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_BUDGETING_BGTCAT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_BUDGETING_BGTCAT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;