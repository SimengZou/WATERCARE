CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLASSETSSEWER_XSEWERSERVICELINE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLASSETSSEWER_XSEWERSERVICELINE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;