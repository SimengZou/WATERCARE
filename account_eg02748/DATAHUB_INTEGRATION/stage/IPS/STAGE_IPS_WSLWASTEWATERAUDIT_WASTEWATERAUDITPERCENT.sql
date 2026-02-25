CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLWASTEWATERAUDIT_WASTEWATERAUDITPERCENT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLWASTEWATERAUDIT_WASTEWATERAUDITPERCENT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;