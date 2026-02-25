CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5SCWORKORDERSREPORTED_ERROR AS SELECT src, 'EAM_R5SCWORKORDERSREPORTED' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWR_DATE), '1900-01-01')) is null and 
                    src:CWR_DATE is not null) THEN
                    'CWR_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CWR_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWR_LASTSAVED), '1900-01-01')) is null and 
                    src:CWR_LASTSAVED is not null) THEN
                    'CWR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CWR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CWR_WOSREPORTED), '0'), 38, 10) is null and 
                    src:CWR_WOSREPORTED is not null) THEN
                    'CWR_WOSREPORTED contains a non-numeric value : \'' || AS_VARCHAR(src:CWR_WOSREPORTED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWR_LASTSAVED), '1900-01-01')) is null and 
                src:CWR_LASTSAVED is not null) THEN
                'CWR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CWR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CWR_DATE,
                src:CWR_MRC,
                src:CWR_ORG  ORDER BY 
            src:CWR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SCWORKORDERSREPORTED as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWR_DATE), '1900-01-01')) is null and 
                    src:CWR_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWR_LASTSAVED), '1900-01-01')) is null and 
                    src:CWR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CWR_WOSREPORTED), '0'), 38, 10) is null and 
                    src:CWR_WOSREPORTED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWR_LASTSAVED), '1900-01-01')) is null and 
                src:CWR_LASTSAVED is not null)