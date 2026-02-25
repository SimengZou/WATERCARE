CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_EAM_R5DWMETASTATUSES
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/EAM/eam_R5DWMETASTATUSES/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;