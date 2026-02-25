CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5UOMS_ERROR AS SELECT src, 'EAM_R5UOMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_CREATED), '1900-01-01')) is null and 
                    src:UOM_CREATED is not null) THEN
                    'UOM_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOM_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_LASTSAVED), '1900-01-01')) is null and 
                    src:UOM_LASTSAVED is not null) THEN
                    'UOM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOM_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UOM_UPDATECOUNT is not null) THEN
                    'UOM_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:UOM_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_UPDATED), '1900-01-01')) is null and 
                    src:UOM_UPDATED is not null) THEN
                    'UOM_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOM_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_LASTSAVED), '1900-01-01')) is null and 
                src:UOM_LASTSAVED is not null) THEN
                'UOM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UOM_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:UOM_CODE  ORDER BY 
            src:UOM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UOMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_CREATED), '1900-01-01')) is null and 
                    src:UOM_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_LASTSAVED), '1900-01-01')) is null and 
                    src:UOM_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UOM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UOM_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_UPDATED), '1900-01-01')) is null and 
                    src:UOM_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UOM_LASTSAVED), '1900-01-01')) is null and 
                src:UOM_LASTSAVED is not null)