CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CASHIERING_REGTRANESCROWPAYMENTLINK
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CASHIERING_REGTRANESCROWPAYMENTLINK/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;