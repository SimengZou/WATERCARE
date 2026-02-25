CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TASKS_ERROR AS SELECT src, 'EAM_R5TASKS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_CREATED), '1900-01-01')) is null and 
                    src:TSK_CREATED is not null) THEN
                    'TSK_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_HOURS), '0'), 38, 10) is null and 
                    src:TSK_HOURS is not null) THEN
                    'TSK_HOURS contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_HOURS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_LASTSAVED), '1900-01-01')) is null and 
                    src:TSK_LASTSAVED is not null) THEN
                    'TSK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_PERSONS), '0'), 38, 10) is null and 
                    src:TSK_PERSONS is not null) THEN
                    'TSK_PERSONS contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_PERSONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_PRICE), '0'), 38, 10) is null and 
                    src:TSK_PRICE is not null) THEN
                    'TSK_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_PRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_REVISION), '0'), 38, 10) is null and 
                    src:TSK_REVISION is not null) THEN
                    'TSK_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE01), '1900-01-01')) is null and 
                    src:TSK_UDFDATE01 is not null) THEN
                    'TSK_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE02), '1900-01-01')) is null and 
                    src:TSK_UDFDATE02 is not null) THEN
                    'TSK_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE03), '1900-01-01')) is null and 
                    src:TSK_UDFDATE03 is not null) THEN
                    'TSK_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE04), '1900-01-01')) is null and 
                    src:TSK_UDFDATE04 is not null) THEN
                    'TSK_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE05), '1900-01-01')) is null and 
                    src:TSK_UDFDATE05 is not null) THEN
                    'TSK_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM01), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM01 is not null) THEN
                    'TSK_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM02), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM02 is not null) THEN
                    'TSK_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM03), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM03 is not null) THEN
                    'TSK_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM04), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM04 is not null) THEN
                    'TSK_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM05), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM05 is not null) THEN
                    'TSK_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TSK_UPDATECOUNT is not null) THEN
                    'TSK_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TSK_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UPDATED), '1900-01-01')) is null and 
                    src:TSK_UPDATED is not null) THEN
                    'TSK_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_LASTSAVED), '1900-01-01')) is null and 
                src:TSK_LASTSAVED is not null) THEN
                'TSK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TSK_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:TSK_CODE,
                src:TSK_REVISION  ORDER BY 
            src:TSK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TASKS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_CREATED), '1900-01-01')) is null and 
                    src:TSK_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_HOURS), '0'), 38, 10) is null and 
                    src:TSK_HOURS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_LASTSAVED), '1900-01-01')) is null and 
                    src:TSK_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_PERSONS), '0'), 38, 10) is null and 
                    src:TSK_PERSONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_PRICE), '0'), 38, 10) is null and 
                    src:TSK_PRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_REVISION), '0'), 38, 10) is null and 
                    src:TSK_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE01), '1900-01-01')) is null and 
                    src:TSK_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE02), '1900-01-01')) is null and 
                    src:TSK_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE03), '1900-01-01')) is null and 
                    src:TSK_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE04), '1900-01-01')) is null and 
                    src:TSK_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UDFDATE05), '1900-01-01')) is null and 
                    src:TSK_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM01), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM02), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM03), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM04), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UDFNUM05), '0'), 38, 10) is null and 
                    src:TSK_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TSK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TSK_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_UPDATED), '1900-01-01')) is null and 
                    src:TSK_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TSK_LASTSAVED), '1900-01-01')) is null and 
                src:TSK_LASTSAVED is not null)