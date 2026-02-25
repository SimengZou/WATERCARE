CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCODESASSETS_QUAKEDESIGN
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCODESASSETS_QUAKEDESIGN/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;