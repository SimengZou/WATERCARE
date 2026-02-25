CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5MOBILEUSERCONFIGS_ERROR AS SELECT src, 'EAM_R5MOBILEUSERCONFIGS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_CREATED), '1900-01-01')) is null and 
                    src:MUC_CREATED is not null) THEN
                    'MUC_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MUC_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_LASTSAVED), '1900-01-01')) is null and 
                    src:MUC_LASTSAVED is not null) THEN
                    'MUC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MUC_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MUC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MUC_UPDATECOUNT is not null) THEN
                    'MUC_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MUC_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_UPDATED), '1900-01-01')) is null and 
                    src:MUC_UPDATED is not null) THEN
                    'MUC_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MUC_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_LASTSAVED), '1900-01-01')) is null and 
                src:MUC_LASTSAVED is not null) THEN
                'MUC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MUC_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:MUC_CODE,
                src:MUC_DESK,
                src:MUC_GROUP,
                src:MUC_USER  ORDER BY 
            src:MUC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILEUSERCONFIGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_CREATED), '1900-01-01')) is null and 
                    src:MUC_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_LASTSAVED), '1900-01-01')) is null and 
                    src:MUC_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MUC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MUC_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_UPDATED), '1900-01-01')) is null and 
                    src:MUC_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MUC_LASTSAVED), '1900-01-01')) is null and 
                src:MUC_LASTSAVED is not null)