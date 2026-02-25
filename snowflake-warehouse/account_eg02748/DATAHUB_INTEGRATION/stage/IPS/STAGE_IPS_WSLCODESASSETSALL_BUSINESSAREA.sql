CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCODESASSETSALL_BUSINESSAREA
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCODESASSETSALL_BUSINESSAREA/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;