CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCDRPLAN_XPLANAPTIMELINEGD/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;