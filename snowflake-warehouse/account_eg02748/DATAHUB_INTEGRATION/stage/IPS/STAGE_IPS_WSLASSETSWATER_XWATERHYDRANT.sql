CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLASSETSWATER_XWATERHYDRANT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLASSETSWATER_XWATERHYDRANT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;