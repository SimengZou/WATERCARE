CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_PORTAL_INVITATION
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_PORTAL_INVITATION/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;