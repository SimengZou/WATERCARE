CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_RESOURCES_TBL327
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_RESOURCES_TBL327/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;