CREATE OR REPLACE NOTIFICATION INTEGRATION ${buildvar.env}_NOTIFICATION_INTEGRATION_PIPE
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:ap-southeast-2:970227736921:${buildvar.sns_topic_notification}-snowflake-snowpipe'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::184570028666:role/role_SnowflakeTasksErrorNotification_poc';