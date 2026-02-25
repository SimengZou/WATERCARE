CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5SHFPERS_ERROR AS SELECT src, 'EAM_R5SHFPERS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_END), '1900-01-01')) is null and 
                    src:SHP_END is not null) THEN
                    'SHP_END contains a non-timestamp value : \'' || AS_VARCHAR(src:SHP_END) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_LASTSAVED), '1900-01-01')) is null and 
                    src:SHP_LASTSAVED is not null) THEN
                    'SHP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SHP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_START), '1900-01-01')) is null and 
                    src:SHP_START is not null) THEN
                    'SHP_START contains a non-timestamp value : \'' || AS_VARCHAR(src:SHP_START) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SHP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SHP_UPDATECOUNT is not null) THEN
                    'SHP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:SHP_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_LASTSAVED), '1900-01-01')) is null and 
                src:SHP_LASTSAVED is not null) THEN
                'SHP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SHP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:SHP_END,
                src:SHP_PERSON,
                src:SHP_SHIFT,
                src:SHP_START  ORDER BY 
            src:SHP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SHFPERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_END), '1900-01-01')) is null and 
                    src:SHP_END is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_LASTSAVED), '1900-01-01')) is null and 
                    src:SHP_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_START), '1900-01-01')) is null and 
                    src:SHP_START is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SHP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SHP_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SHP_LASTSAVED), '1900-01-01')) is null and 
                src:SHP_LASTSAVED is not null)