CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_RESOURCES_BUDGETSEGMENTDEF
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_RESOURCES_BUDGETSEGMENTDEF/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;