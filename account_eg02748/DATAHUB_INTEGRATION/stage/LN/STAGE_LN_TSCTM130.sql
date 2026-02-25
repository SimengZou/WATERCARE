CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_LN_TSCTM130
                        url = 's3://wsl-${buildvar.env_lower}-deinf-ln-landing-s3/LN/LN_tsctm130/'
                    storage_integration = ${buildvar.env}_WSL_LN_S3;