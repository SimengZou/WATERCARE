CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_RESOURCES_CREDITRATINGLEVEL
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_RESOURCES_CREDITRATINGLEVEL/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;