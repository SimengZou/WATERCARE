CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_EAM_R5URLPARAMETERS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/EAM/eam_R5URLPARAMETERS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;