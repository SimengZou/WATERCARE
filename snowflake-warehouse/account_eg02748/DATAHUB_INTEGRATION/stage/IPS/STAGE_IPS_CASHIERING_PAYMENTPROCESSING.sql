CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CASHIERING_PAYMENTPROCESSING
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CASHIERING_PAYMENTPROCESSING/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;