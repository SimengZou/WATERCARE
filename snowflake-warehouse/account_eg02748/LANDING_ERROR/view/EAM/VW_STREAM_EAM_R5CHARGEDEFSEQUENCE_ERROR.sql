CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CHARGEDEFSEQUENCE_ERROR AS SELECT src, 'EAM_R5CHARGEDEFSEQUENCE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CDS_LASTSAVED), '1900-01-01')) is null and 
                    src:CDS_LASTSAVED is not null) THEN
                    'CDS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CDS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDS_SEQUENCE), '0'), 38, 10) is null and 
                    src:CDS_SEQUENCE is not null) THEN
                    'CDS_SEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:CDS_SEQUENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CDS_UPDATECOUNT is not null) THEN
                    'CDS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CDS_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CDS_LASTSAVED), '1900-01-01')) is null and 
                src:CDS_LASTSAVED is not null) THEN
                'CDS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CDS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CDS_CATEGORY,
                src:CDS_LEVEL,
                src:CDS_SEQUENCE,
                src:CDS_SUBCATEGORY  ORDER BY 
            src:CDS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CHARGEDEFSEQUENCE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CDS_LASTSAVED), '1900-01-01')) is null and 
                    src:CDS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDS_SEQUENCE), '0'), 38, 10) is null and 
                    src:CDS_SEQUENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CDS_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CDS_LASTSAVED), '1900-01-01')) is null and 
                src:CDS_LASTSAVED is not null)