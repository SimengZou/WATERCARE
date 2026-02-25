CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CASHIERING_CASHREGISTERTYPE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CASHIERING_CASHREGISTERTYPE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;