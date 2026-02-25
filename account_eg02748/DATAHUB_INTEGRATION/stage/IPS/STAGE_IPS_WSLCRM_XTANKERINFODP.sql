CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCRM_XTANKERINFODP
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCRM_XTANKERINFODP/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;