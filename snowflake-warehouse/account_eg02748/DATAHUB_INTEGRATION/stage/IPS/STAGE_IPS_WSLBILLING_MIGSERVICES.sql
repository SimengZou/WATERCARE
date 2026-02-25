CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLBILLING_MIGSERVICES
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLBILLING_MIGSERVICES/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;