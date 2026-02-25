CREATE OR REPLACE STAGE DATAHUB_EXTRACT.STAGE_IPS_SM_BILLING
  url='s3://${buildvar.infors3bucket}-ips-smartmeter/ReadingFiles/'
  storage_integration = ${buildvar.env}_WSL_IPSSMBILLING_S3;					