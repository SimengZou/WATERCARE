CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLASSETSOTHER_XOFFICEEQUIPMENT
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLASSETSOTHER_XOFFICEEQUIPMENT/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;