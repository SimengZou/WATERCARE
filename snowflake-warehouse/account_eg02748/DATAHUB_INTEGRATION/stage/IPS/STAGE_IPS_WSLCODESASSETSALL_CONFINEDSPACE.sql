CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCODESASSETSALL_CONFINEDSPACE
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCODESASSETSALL_CONFINEDSPACE/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;