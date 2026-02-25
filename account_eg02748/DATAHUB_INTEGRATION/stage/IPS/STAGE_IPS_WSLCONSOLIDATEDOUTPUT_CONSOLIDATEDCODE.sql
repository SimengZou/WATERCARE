CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCONSOLIDATEDOUTPUT_CONSOLIDATEDCODE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCONSOLIDATEDOUTPUT_CONSOLIDATEDCODE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;