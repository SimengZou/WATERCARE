CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_CDR_APGRP
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_CDR_APGRP/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;