CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5LOTS_ERROR AS SELECT src, 'EAM_R5LOTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOT_EXPIRED), '1900-01-01')) is null and 
                    src:LOT_EXPIRED is not null) THEN
                    'LOT_EXPIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:LOT_EXPIRED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOT_LASTSAVED), '1900-01-01')) is null and 
                    src:LOT_LASTSAVED is not null) THEN
                    'LOT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LOT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:LOT_UPDATECOUNT is not null) THEN
                    'LOT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:LOT_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOT_LASTSAVED), '1900-01-01')) is null and 
                src:LOT_LASTSAVED is not null) THEN
                'LOT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LOT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:LOT_CODE  ORDER BY 
            src:LOT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LOTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOT_EXPIRED), '1900-01-01')) is null and 
                    src:LOT_EXPIRED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOT_LASTSAVED), '1900-01-01')) is null and 
                    src:LOT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:LOT_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOT_LASTSAVED), '1900-01-01')) is null and 
                src:LOT_LASTSAVED is not null)