CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_EAM_R5HOMEUSERS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/EAM/eam_R5HOMEUSERS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;