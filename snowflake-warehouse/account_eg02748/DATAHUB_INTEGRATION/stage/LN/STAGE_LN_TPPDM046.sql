CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_LN_TPPDM046
                        url = 's3://wsl-${buildvar.env_lower}-deinf-ln-landing-s3/LN/LN_tppdm046/'
                    storage_integration = ${buildvar.env}_WSL_LN_S3;