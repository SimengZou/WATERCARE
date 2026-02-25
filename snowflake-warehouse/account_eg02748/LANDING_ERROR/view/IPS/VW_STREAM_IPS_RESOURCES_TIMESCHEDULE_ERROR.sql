CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_TIMESCHEDULE_ERROR AS SELECT src, 'IPS_RESOURCES_TIMESCHEDULE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYFLAGS), '0'), 38, 10) is null and 
                    src:DAYFLAGS is not null) THEN
                    'DAYFLAGS contains a non-numeric value : \'' || AS_VARCHAR(src:DAYFLAGS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYOFMONTH), '0'), 38, 10) is null and 
                    src:DAYOFMONTH is not null) THEN
                    'DAYOFMONTH contains a non-numeric value : \'' || AS_VARCHAR(src:DAYOFMONTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYOFWEEK), '0'), 38, 10) is null and 
                    src:DAYOFWEEK is not null) THEN
                    'DAYOFWEEK contains a non-numeric value : \'' || AS_VARCHAR(src:DAYOFWEEK) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSADV), '0'), 38, 10) is null and 
                    src:MAXDAYSADV is not null) THEN
                    'MAXDAYSADV contains a non-numeric value : \'' || AS_VARCHAR(src:MAXDAYSADV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MIGSRCKEY), '0'), 38, 10) is null and 
                    src:MIGSRCKEY is not null) THEN
                    'MIGSRCKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MIGSRCKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINDAYSADV), '0'), 38, 10) is null and 
                    src:MINDAYSADV is not null) THEN
                    'MINDAYSADV contains a non-numeric value : \'' || AS_VARCHAR(src:MINDAYSADV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MONTHOFYEAR), '0'), 38, 10) is null and 
                    src:MONTHOFYEAR is not null) THEN
                    'MONTHOFYEAR contains a non-numeric value : \'' || AS_VARCHAR(src:MONTHOFYEAR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTSCHEDDATE), '1900-01-01')) is null and 
                    src:NEXTSCHEDDATE is not null) THEN
                    'NEXTSCHEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEXTSCHEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATHOURS), '0'), 38, 10) is null and 
                    src:REPEATHOURS is not null) THEN
                    'REPEATHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:REPEATHOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATMINUTES), '0'), 38, 10) is null and 
                    src:REPEATMINUTES is not null) THEN
                    'REPEATMINUTES contains a non-numeric value : \'' || AS_VARCHAR(src:REPEATMINUTES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATUNTILHOURS), '0'), 38, 10) is null and 
                    src:REPEATUNTILHOURS is not null) THEN
                    'REPEATUNTILHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:REPEATUNTILHOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATUNTILMIN), '0'), 38, 10) is null and 
                    src:REPEATUNTILMIN is not null) THEN
                    'REPEATUNTILMIN contains a non-numeric value : \'' || AS_VARCHAR(src:REPEATUNTILMIN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REPEATUNTILTIME), '1900-01-01')) is null and 
                    src:REPEATUNTILTIME is not null) THEN
                    'REPEATUNTILTIME contains a non-timestamp value : \'' || AS_VARCHAR(src:REPEATUNTILTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDTYPE), '0'), 38, 10) is null and 
                    src:SCHEDTYPE is not null) THEN
                    'SCHEDTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SKIPDAYS), '0'), 38, 10) is null and 
                    src:SKIPDAYS is not null) THEN
                    'SKIPDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:SKIPDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SKIPMONTHS), '0'), 38, 10) is null and 
                    src:SKIPMONTHS is not null) THEN
                    'SKIPMONTHS contains a non-numeric value : \'' || AS_VARCHAR(src:SKIPMONTHS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SKIPWEEKS), '0'), 38, 10) is null and 
                    src:SKIPWEEKS is not null) THEN
                    'SKIPWEEKS contains a non-numeric value : \'' || AS_VARCHAR(src:SKIPWEEKS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDATE), '1900-01-01')) is null and 
                    src:STARTDATE is not null) THEN
                    'STARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TIMESCHEDKEY), '0'), 38, 10) is null and 
                    src:TIMESCHEDKEY is not null) THEN
                    'TIMESCHEDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:TIMESCHEDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEEKFLAGS), '0'), 38, 10) is null and 
                    src:WEEKFLAGS is not null) THEN
                    'WEEKFLAGS contains a non-numeric value : \'' || AS_VARCHAR(src:WEEKFLAGS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEEKOFMONTH), '0'), 38, 10) is null and 
                    src:WEEKOFMONTH is not null) THEN
                    'WEEKOFMONTH contains a non-numeric value : \'' || AS_VARCHAR(src:WEEKOFMONTH) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:YEARLYDATE), '1900-01-01')) is null and 
                    src:YEARLYDATE is not null) THEN
                    'YEARLYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:YEARLYDATE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
                etl_load_datetime,
                etl_load_metadatafilename
                FROM 
                (
                select 
                    src,
                    etl_load_datetime,
                    etl_load_metadatafilename
                    from
                    (
                        SELECT
        
                            
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:TIMESCHEDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_TIMESCHEDULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYFLAGS), '0'), 38, 10) is null and 
                    src:DAYFLAGS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYOFMONTH), '0'), 38, 10) is null and 
                    src:DAYOFMONTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYOFWEEK), '0'), 38, 10) is null and 
                    src:DAYOFWEEK is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSADV), '0'), 38, 10) is null and 
                    src:MAXDAYSADV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MIGSRCKEY), '0'), 38, 10) is null and 
                    src:MIGSRCKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINDAYSADV), '0'), 38, 10) is null and 
                    src:MINDAYSADV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MONTHOFYEAR), '0'), 38, 10) is null and 
                    src:MONTHOFYEAR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTSCHEDDATE), '1900-01-01')) is null and 
                    src:NEXTSCHEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATHOURS), '0'), 38, 10) is null and 
                    src:REPEATHOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATMINUTES), '0'), 38, 10) is null and 
                    src:REPEATMINUTES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATUNTILHOURS), '0'), 38, 10) is null and 
                    src:REPEATUNTILHOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPEATUNTILMIN), '0'), 38, 10) is null and 
                    src:REPEATUNTILMIN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REPEATUNTILTIME), '1900-01-01')) is null and 
                    src:REPEATUNTILTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDTYPE), '0'), 38, 10) is null and 
                    src:SCHEDTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SKIPDAYS), '0'), 38, 10) is null and 
                    src:SKIPDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SKIPMONTHS), '0'), 38, 10) is null and 
                    src:SKIPMONTHS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SKIPWEEKS), '0'), 38, 10) is null and 
                    src:SKIPWEEKS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDATE), '1900-01-01')) is null and 
                    src:STARTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TIMESCHEDKEY), '0'), 38, 10) is null and 
                    src:TIMESCHEDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEEKFLAGS), '0'), 38, 10) is null and 
                    src:WEEKFLAGS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEEKOFMONTH), '0'), 38, 10) is null and 
                    src:WEEKOFMONTH is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:YEARLYDATE), '1900-01-01')) is null and 
                    src:YEARLYDATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)