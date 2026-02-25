CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLSERVICEREQUESTCODES_SRSTATUS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLSERVICEREQUESTCODES_SRSTATUS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;