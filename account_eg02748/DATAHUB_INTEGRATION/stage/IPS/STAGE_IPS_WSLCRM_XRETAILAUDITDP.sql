CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCRM_XRETAILAUDITDP
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCRM_XRETAILAUDITDP/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;