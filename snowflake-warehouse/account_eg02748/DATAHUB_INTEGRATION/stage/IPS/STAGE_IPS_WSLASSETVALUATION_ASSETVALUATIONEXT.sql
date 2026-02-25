CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLASSETVALUATION_ASSETVALUATIONEXT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLASSETVALUATION_ASSETVALUATIONEXT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;