CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_METERMANAGEMENT_WATER_WATERMETERREADINGBATCH
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_METERMANAGEMENT_WATER_WATERMETERREADINGBATCH/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;