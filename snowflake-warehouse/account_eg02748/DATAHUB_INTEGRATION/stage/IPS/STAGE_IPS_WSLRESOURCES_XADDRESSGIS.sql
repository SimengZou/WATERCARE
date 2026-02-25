CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLRESOURCES_XADDRESSGIS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLRESOURCES_XADDRESSGIS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;