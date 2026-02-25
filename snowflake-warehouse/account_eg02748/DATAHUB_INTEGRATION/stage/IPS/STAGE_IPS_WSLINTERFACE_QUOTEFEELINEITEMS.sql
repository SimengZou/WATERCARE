CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLINTERFACE_QUOTEFEELINEITEMS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLINTERFACE_QUOTEFEELINEITEMS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;