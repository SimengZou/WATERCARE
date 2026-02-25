CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ACTSCHEDULES_ERROR AS SELECT src, 'EAM_R5ACTSCHEDULES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_ACTIVITY), '0'), 38, 10) is null and 
                    src:ACS_ACTIVITY is not null) THEN
                    'ACS_ACTIVITY contains a non-numeric value : \'' || AS_VARCHAR(src:ACS_ACTIVITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_ENDTIME), '0'), 38, 10) is null and 
                    src:ACS_ENDTIME is not null) THEN
                    'ACS_ENDTIME contains a non-numeric value : \'' || AS_VARCHAR(src:ACS_ENDTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_HOURS), '0'), 38, 10) is null and 
                    src:ACS_HOURS is not null) THEN
                    'ACS_HOURS contains a non-numeric value : \'' || AS_VARCHAR(src:ACS_HOURS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_LASTSAVED), '1900-01-01')) is null and 
                    src:ACS_LASTSAVED is not null) THEN
                    'ACS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_SCHED), '1900-01-01')) is null and 
                    src:ACS_SCHED is not null) THEN
                    'ACS_SCHED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACS_SCHED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_SHIFTSCHEDULESESSION), '0'), 38, 10) is null and 
                    src:ACS_SHIFTSCHEDULESESSION is not null) THEN
                    'ACS_SHIFTSCHEDULESESSION contains a non-numeric value : \'' || AS_VARCHAR(src:ACS_SHIFTSCHEDULESESSION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_STARTTIME), '0'), 38, 10) is null and 
                    src:ACS_STARTTIME is not null) THEN
                    'ACS_STARTTIME contains a non-numeric value : \'' || AS_VARCHAR(src:ACS_STARTTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ACS_UPDATECOUNT is not null) THEN
                    'ACS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ACS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_UPDATED), '1900-01-01')) is null and 
                    src:ACS_UPDATED is not null) THEN
                    'ACS_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACS_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_LASTSAVED), '1900-01-01')) is null and 
                src:ACS_LASTSAVED is not null) THEN
                'ACS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ACS_CODE  ORDER BY 
            src:ACS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACTSCHEDULES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_ACTIVITY), '0'), 38, 10) is null and 
                    src:ACS_ACTIVITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_ENDTIME), '0'), 38, 10) is null and 
                    src:ACS_ENDTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_HOURS), '0'), 38, 10) is null and 
                    src:ACS_HOURS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_LASTSAVED), '1900-01-01')) is null and 
                    src:ACS_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_SCHED), '1900-01-01')) is null and 
                    src:ACS_SCHED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_SHIFTSCHEDULESESSION), '0'), 38, 10) is null and 
                    src:ACS_SHIFTSCHEDULESESSION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_STARTTIME), '0'), 38, 10) is null and 
                    src:ACS_STARTTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ACS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_UPDATED), '1900-01-01')) is null and 
                    src:ACS_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACS_LASTSAVED), '1900-01-01')) is null and 
                src:ACS_LASTSAVED is not null)