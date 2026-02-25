CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLPORTAL_XSUBURBS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLPORTAL_XSUBURBS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;