CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DOCSIZES_ERROR AS SELECT src, 'EAM_R5DOCSIZES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DSZ_LASTSAVED), '1900-01-01')) is null and 
                    src:DSZ_LASTSAVED is not null) THEN
                    'DSZ_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DSZ_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSZ_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DSZ_UPDATECOUNT is not null) THEN
                    'DSZ_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DSZ_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DSZ_LASTSAVED), '1900-01-01')) is null and 
                src:DSZ_LASTSAVED is not null) THEN
                'DSZ_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DSZ_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DSZ_CODE  ORDER BY 
            src:DSZ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCSIZES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DSZ_LASTSAVED), '1900-01-01')) is null and 
                    src:DSZ_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSZ_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DSZ_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DSZ_LASTSAVED), '1900-01-01')) is null and 
                src:DSZ_LASTSAVED is not null)