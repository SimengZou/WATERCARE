CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_ASSETMANAGEMENT_WATER_WATERAREA
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_ASSETMANAGEMENT_WATER_WATERAREA/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;