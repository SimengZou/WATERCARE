CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCODES_LANDLINETYPE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCODES_LANDLINETYPE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;