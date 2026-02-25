CREATE OR REPLACE STAGE DATAHUB_INTEGRATION.STAGE_SM_CURATED
url = 's3://${buildvar.smcurated}/' 
storage_integration = ${buildvar.env}_WSL_APSY_${buildvar.env}_SMINF_CUR_S3;