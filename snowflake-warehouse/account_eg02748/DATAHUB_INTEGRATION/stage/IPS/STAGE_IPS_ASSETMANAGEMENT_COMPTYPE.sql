CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_ASSETMANAGEMENT_COMPTYPE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_ASSETMANAGEMENT_COMPTYPE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;