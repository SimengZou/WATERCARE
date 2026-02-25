CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_EAM_U5ASSETSUBTYPES
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/EAM/eam_U5ASSETSUBTYPES/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;