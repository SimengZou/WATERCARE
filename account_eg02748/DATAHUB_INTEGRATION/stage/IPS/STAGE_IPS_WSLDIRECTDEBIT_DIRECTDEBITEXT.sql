CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLDIRECTDEBIT_DIRECTDEBITEXT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLDIRECTDEBIT_DIRECTDEBITEXT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;