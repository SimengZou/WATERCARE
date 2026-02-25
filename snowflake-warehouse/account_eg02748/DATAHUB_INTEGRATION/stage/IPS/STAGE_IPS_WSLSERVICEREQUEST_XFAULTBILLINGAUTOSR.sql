CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;