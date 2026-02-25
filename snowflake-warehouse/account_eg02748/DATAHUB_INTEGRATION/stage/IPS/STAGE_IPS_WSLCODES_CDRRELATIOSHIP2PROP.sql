CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCODES_CDRRELATIOSHIP2PROP
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCODES_CDRRELATIOSHIP2PROP/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;