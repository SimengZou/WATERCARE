CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL
                        url = 's3://${buildvar.infors3bucket}-datalake-inforapptables/IPS/IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL/'
                    storage_integration = ${buildvar.env}_WSL_SNOWFLAKE_S3_INTEGRATION;