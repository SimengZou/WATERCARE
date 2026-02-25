CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCDRBUILD_FEESNONDOMCODES
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCDRBUILD_FEESNONDOMCODES/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;