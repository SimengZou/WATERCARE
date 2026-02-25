CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCUSTOMER_XIDENTITYCR
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCUSTOMER_XIDENTITYCR/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;