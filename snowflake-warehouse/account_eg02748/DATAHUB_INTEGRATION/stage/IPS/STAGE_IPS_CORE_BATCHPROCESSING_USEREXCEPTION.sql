CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CORE_BATCHPROCESSING_USEREXCEPTION
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CORE_BATCHPROCESSING_USEREXCEPTION/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;