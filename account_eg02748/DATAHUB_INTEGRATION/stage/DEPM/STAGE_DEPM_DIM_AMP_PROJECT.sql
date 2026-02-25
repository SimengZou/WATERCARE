CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_DEPM_DIM_AMP_PROJECT
                        url = 's3://wsl-${buildvar.env_lower}-deinf-depm-landing-s3/dEPM/depm_dim_AMP_Project/'
                    storage_integration = ${buildvar.env}_WSL_DEPM_S3;