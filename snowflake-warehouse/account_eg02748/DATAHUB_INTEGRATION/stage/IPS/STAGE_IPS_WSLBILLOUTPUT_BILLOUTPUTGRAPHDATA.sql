CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLBILLOUTPUT_BILLOUTPUTGRAPHDATA
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLBILLOUTPUT_BILLOUTPUTGRAPHDATA/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;