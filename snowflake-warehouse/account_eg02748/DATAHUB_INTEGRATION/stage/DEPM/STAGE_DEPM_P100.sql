CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_DEPM_P100
                        url = 's3://wsl-${buildvar.env_lower}-deinf-depm-landing-s3/dEPM/dEPM_P100/'
                    storage_integration = ${buildvar.env}_WSL_DEPM_S3;