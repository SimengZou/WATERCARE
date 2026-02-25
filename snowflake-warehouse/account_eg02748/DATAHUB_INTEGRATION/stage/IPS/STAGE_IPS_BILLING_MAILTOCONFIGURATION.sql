CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_BILLING_MAILTOCONFIGURATION
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_BILLING_MAILTOCONFIGURATION/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;