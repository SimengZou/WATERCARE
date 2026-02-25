CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CRM_CRMRESPONSIBILITY
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CRM_CRMRESPONSIBILITY/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;