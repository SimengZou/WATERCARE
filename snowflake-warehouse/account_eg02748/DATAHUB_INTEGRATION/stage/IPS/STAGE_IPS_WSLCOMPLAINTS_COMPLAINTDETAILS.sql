CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_WSLCOMPLAINTS_COMPLAINTDETAILS
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_WSLCOMPLAINTS_COMPLAINTDETAILS/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;