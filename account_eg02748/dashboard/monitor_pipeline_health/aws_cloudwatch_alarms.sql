SELECT SRC:AlarmName::string AS "ALARM NAME",
    UPPER(SPLIT_PART(SRC:AlarmName::string, '-', 4)) AS "SOURCE",    
    SRC:AlarmDescription::string AS "ALARM DESCRIPTION",
    :datebucket(TO_TIMESTAMP(SRC:StateChangeTime::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"+0000')) AS "STATE CHANGE TIME",
    SRC:Trigger:Namespace::string AS "NAMESPACE",
    SRC:Trigger:MetricName::string AS "METRIC NAME",
    COUNT(*) AS "ALARM COUNT"
FROM WATERLAKE_${var.ENVIRONMENT}.LOGGING.S3_CLOUDWATCH_ALARMS
WHERE "SOURCE" <> 'ATHENA'
AND "SOURCE" = :source_system
AND "STATE CHANGE TIME" = :daterange
GROUP BY "ALARM NAME",
    "SOURCE",
    "ALARM DESCRIPTION",
    :datebucket(TO_TIMESTAMP(SRC:StateChangeTime::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"+0000')),
    "NAMESPACE",
    "METRIC NAME"
ORDER BY "STATE CHANGE TIME";