CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_EAM_U5WAREHOUSES
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/EAM/eam_U5WAREHOUSES/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;