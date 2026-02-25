CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_LN_TDPUR406
                        url = 's3://wsl-${buildvar.env_lower}-deinf-ln-landing-s3/LN/LN_tdpur406/'
                    storage_integration = ${buildvar.env}_WSL_LN_S3;