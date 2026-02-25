CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_U5ASSETSUBTYPES_ERROR AS SELECT src, 'EAM_U5ASSETSUBTYPES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATED), '1900-01-01')) is null and 
                    src:CREATED is not null) THEN
                    'CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                    src:LASTSAVED is not null) THEN
                    'LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UPDATECOUNT is not null) THEN
                    'UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UPDATED), '1900-01-01')) is null and 
                    src:UPDATED is not null) THEN
                    'UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                src:LASTSAVED is not null) THEN
                'LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:AST_ASSETSUBTYPE,
                src:AST_ASSETTYPE  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5ASSETSUBTYPES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATED), '1900-01-01')) is null and 
                    src:CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                    src:LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UPDATED), '1900-01-01')) is null and 
                    src:UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                src:LASTSAVED is not null)