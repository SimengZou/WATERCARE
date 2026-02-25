CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLTRADEBILLOUTPUT_TRADEBILLOUTPUTTASKLOG
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLTRADEBILLOUTPUT_TRADEBILLOUTPUTTASKLOG/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;