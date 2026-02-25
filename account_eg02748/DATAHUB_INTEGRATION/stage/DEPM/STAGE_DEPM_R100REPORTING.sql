CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_DEPM_R100REPORTING
                        url = 's3://wsl-${buildvar.env_lower}-deinf-depm-landing-s3/dEPM/dEPM_R100Reporting/'
                    storage_integration = ${buildvar.env}_WSL_DEPM_S3;