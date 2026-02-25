CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_LN_TSCLM335
                        url = 's3://wsl-${buildvar.env_lower}-deinf-ln-landing-s3/LN/LN_tsclm335/'
                    storage_integration = ${buildvar.env}_WSL_LN_S3;