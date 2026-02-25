CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_PROPERTY_ADDRCNTC
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_PROPERTY_ADDRCNTC/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;