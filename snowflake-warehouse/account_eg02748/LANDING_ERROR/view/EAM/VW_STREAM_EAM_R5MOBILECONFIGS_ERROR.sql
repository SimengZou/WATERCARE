CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5MOBILECONFIGS_ERROR AS SELECT src, 'EAM_R5MOBILECONFIGS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MCF_CONFIG), '0'), 38, 10) is null and 
                    src:MCF_CONFIG is not null) THEN
                    'MCF_CONFIG contains a non-numeric value : \'' || AS_VARCHAR(src:MCF_CONFIG) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_CREATED), '1900-01-01')) is null and 
                    src:MCF_CREATED is not null) THEN
                    'MCF_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MCF_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_LASTSAVED), '1900-01-01')) is null and 
                    src:MCF_LASTSAVED is not null) THEN
                    'MCF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MCF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MCF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MCF_UPDATECOUNT is not null) THEN
                    'MCF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MCF_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_UPDATED), '1900-01-01')) is null and 
                    src:MCF_UPDATED is not null) THEN
                    'MCF_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:MCF_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_LASTSAVED), '1900-01-01')) is null and 
                src:MCF_LASTSAVED is not null) THEN
                'MCF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MCF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:MCF_CODE,
                src:MCF_CONFIG,
                src:MCF_DESK,
                src:MCF_RCODE  ORDER BY 
            src:MCF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILECONFIGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MCF_CONFIG), '0'), 38, 10) is null and 
                    src:MCF_CONFIG is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_CREATED), '1900-01-01')) is null and 
                    src:MCF_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_LASTSAVED), '1900-01-01')) is null and 
                    src:MCF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MCF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MCF_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_UPDATED), '1900-01-01')) is null and 
                    src:MCF_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MCF_LASTSAVED), '1900-01-01')) is null and 
                src:MCF_LASTSAVED is not null)