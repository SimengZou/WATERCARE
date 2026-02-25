CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_LN_TDSMI007
                        url = 's3://wsl-${buildvar.env_lower}-deinf-ln-landing-s3/LN/LN_tdsmi007/'
                    storage_integration = ${buildvar.env}_WSL_LN_S3;