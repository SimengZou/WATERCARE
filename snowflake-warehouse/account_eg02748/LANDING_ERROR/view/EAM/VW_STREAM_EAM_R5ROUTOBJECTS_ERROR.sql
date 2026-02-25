CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ROUTOBJECTS_ERROR AS SELECT src, 'EAM_R5ROUTOBJECTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_FROMPOINT), '0'), 38, 10) is null and 
                    src:ROB_FROMPOINT is not null) THEN
                    'ROB_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:ROB_FROM_OFFSET is not null) THEN
                    'ROB_FROM_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_FROM_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:ROB_FROM_REFERENCE is not null) THEN
                    'ROB_FROM_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_FROM_REFERENCE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROB_LASTSAVED), '1900-01-01')) is null and 
                    src:ROB_LASTSAVED is not null) THEN
                    'ROB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ROB_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_LINE), '0'), 38, 10) is null and 
                    src:ROB_LINE is not null) THEN
                    'ROB_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_REVISION), '0'), 38, 10) is null and 
                    src:ROB_REVISION is not null) THEN
                    'ROB_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_TOPOINT), '0'), 38, 10) is null and 
                    src:ROB_TOPOINT is not null) THEN
                    'ROB_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_TOPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_TO_OFFSET), '0'), 38, 10) is null and 
                    src:ROB_TO_OFFSET is not null) THEN
                    'ROB_TO_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_TO_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:ROB_TO_REFERENCE is not null) THEN
                    'ROB_TO_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_TO_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ROB_UPDATECOUNT is not null) THEN
                    'ROB_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ROB_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROB_LASTSAVED), '1900-01-01')) is null and 
                src:ROB_LASTSAVED is not null) THEN
                'ROB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ROB_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ROB_LINE,
                src:ROB_REVISION,
                src:ROB_ROUTE  ORDER BY 
            src:ROB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ROUTOBJECTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_FROMPOINT), '0'), 38, 10) is null and 
                    src:ROB_FROMPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:ROB_FROM_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:ROB_FROM_REFERENCE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROB_LASTSAVED), '1900-01-01')) is null and 
                    src:ROB_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_LINE), '0'), 38, 10) is null and 
                    src:ROB_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_REVISION), '0'), 38, 10) is null and 
                    src:ROB_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_TOPOINT), '0'), 38, 10) is null and 
                    src:ROB_TOPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_TO_OFFSET), '0'), 38, 10) is null and 
                    src:ROB_TO_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:ROB_TO_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ROB_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ROB_LASTSAVED), '1900-01-01')) is null and 
                src:ROB_LASTSAVED is not null)