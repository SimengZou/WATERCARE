create OR REPLACE storage integration DEV_WSL_IPSSMBILLING_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::566933467089:role/wsl-role-datahub-snowpipe-access'
  storage_allowed_locations = ('s3://watercaredev-ips-smartmeter/')

create OR REPLACE storage integration QA_WSL_IPSSMBILLING_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::917737136967:role/wsl-role-datahub-snowpipe-access'
  storage_allowed_locations = ('s3://watercareqa-ips-smartmeter/')

create OR REPLACE storage integration STG_WSL_IPSSMBILLING_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::160555527957:role/wsl-role-datahub-snowpipe-access'
  storage_allowed_locations = ('s3://watercareuat-ips-smartmeter/')

create OR REPLACE storage integration PRD_WSL_IPSSMBILLING_S3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::213776003548:role/wsl-role-datahub-snowpipe-access'
  storage_allowed_locations = ('s3://watercareprd-ips-smartmeter/')




