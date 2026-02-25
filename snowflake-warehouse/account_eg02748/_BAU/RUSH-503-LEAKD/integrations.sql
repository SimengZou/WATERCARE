create OR REPLACE storage integration DEV_WSL_SMAPP_LEAKDETECTION_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::184570028666:role/wsl-dev-deinf-snowflakeaccess-role'
  storage_allowed_locations = ('s3://wsl-dev-deinf-smapp-leakdetection-s3/');

create OR REPLACE storage integration QA_WSL_SMAPP_LEAKDETECTION_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::082381419916:role/wsl-dev-deinf-snowflakeaccess-role'
  storage_allowed_locations = ('s3://wsl-qa-deinf-smapp-leakdetection-s3/');

create OR REPLACE storage integration STG_WSL_SMAPP_LEAKDETECTION_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::239475850115:role/wsl-dev-deinf-snowflakeaccess-role'
  storage_allowed_locations = ('s3://wsl-stg-deinf-smapp-leakdetection-s3/');

create OR REPLACE storage integration PRD_WSL_SMAPP_LEAKDETECTION_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::014653824585:role/wsl-dev-deinf-snowflakeaccess-role'
  storage_allowed_locations = ('s3://wsl-prd-deinf-smapp-leakdetection-s3/');
