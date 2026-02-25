CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CASHIERING_CASHREGISTER
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CASHIERING_CASHREGISTER/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;