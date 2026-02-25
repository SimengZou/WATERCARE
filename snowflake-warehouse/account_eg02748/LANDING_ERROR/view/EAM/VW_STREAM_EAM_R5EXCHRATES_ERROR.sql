CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EXCHRATES_ERROR AS SELECT src, 'EAM_R5EXCHRATES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_END), '1900-01-01')) is null and 
                    src:CRR_END is not null) THEN
                    'CRR_END contains a non-timestamp value : \'' || AS_VARCHAR(src:CRR_END) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CRR_EXCH), '0'), 38, 10) is null and 
                    src:CRR_EXCH is not null) THEN
                    'CRR_EXCH contains a non-numeric value : \'' || AS_VARCHAR(src:CRR_EXCH) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_INTERFACE), '1900-01-01')) is null and 
                    src:CRR_INTERFACE is not null) THEN
                    'CRR_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:CRR_INTERFACE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_LASTSAVED), '1900-01-01')) is null and 
                    src:CRR_LASTSAVED is not null) THEN
                    'CRR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CRR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_START), '1900-01-01')) is null and 
                    src:CRR_START is not null) THEN
                    'CRR_START contains a non-timestamp value : \'' || AS_VARCHAR(src:CRR_START) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CRR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CRR_UPDATECOUNT is not null) THEN
                    'CRR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CRR_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_LASTSAVED), '1900-01-01')) is null and 
                src:CRR_LASTSAVED is not null) THEN
                'CRR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CRR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CRR_CODE  ORDER BY 
            src:CRR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EXCHRATES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_END), '1900-01-01')) is null and 
                    src:CRR_END is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CRR_EXCH), '0'), 38, 10) is null and 
                    src:CRR_EXCH is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_INTERFACE), '1900-01-01')) is null and 
                    src:CRR_INTERFACE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_LASTSAVED), '1900-01-01')) is null and 
                    src:CRR_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_START), '1900-01-01')) is null and 
                    src:CRR_START is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CRR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CRR_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CRR_LASTSAVED), '1900-01-01')) is null and 
                src:CRR_LASTSAVED is not null)