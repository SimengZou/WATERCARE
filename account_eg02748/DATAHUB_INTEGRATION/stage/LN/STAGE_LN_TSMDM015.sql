CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_LN_TSMDM015
                        url = 's3://wsl-${buildvar.env_lower}-deinf-ln-landing-s3/LN/LN_tsmdm015/'
                    storage_integration = ${buildvar.env}_WSL_LN_S3;