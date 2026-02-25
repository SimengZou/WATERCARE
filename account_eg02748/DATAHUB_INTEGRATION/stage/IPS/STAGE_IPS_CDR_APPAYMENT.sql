CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CDR_APPAYMENT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CDR_APPAYMENT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;