CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCDRBUILD_FEESDOMCODE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCDRBUILD_FEESDOMCODE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;