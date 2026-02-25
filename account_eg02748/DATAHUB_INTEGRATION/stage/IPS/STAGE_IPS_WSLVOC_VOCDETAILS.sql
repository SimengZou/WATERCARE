CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLVOC_VOCDETAILS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLVOC_VOCDETAILS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;