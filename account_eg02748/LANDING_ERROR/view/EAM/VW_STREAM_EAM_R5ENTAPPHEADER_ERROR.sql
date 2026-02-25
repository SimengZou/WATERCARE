CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ENTAPPHEADER_ERROR AS SELECT src, 'EAM_R5ENTAPPHEADER' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_APPDATE), '1900-01-01')) is null and 
                    src:EAH_APPDATE is not null) THEN
                    'EAH_APPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EAH_APPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_DATE), '1900-01-01')) is null and 
                    src:EAH_DATE is not null) THEN
                    'EAH_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EAH_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_LASTSAVED), '1900-01-01')) is null and 
                    src:EAH_LASTSAVED is not null) THEN
                    'EAH_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EAH_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EAH_PK), '0'), 38, 10) is null and 
                    src:EAH_PK is not null) THEN
                    'EAH_PK contains a non-numeric value : \'' || AS_VARCHAR(src:EAH_PK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EAH_REVISION), '0'), 38, 10) is null and 
                    src:EAH_REVISION is not null) THEN
                    'EAH_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:EAH_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EAH_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EAH_UPDATECOUNT is not null) THEN
                    'EAH_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:EAH_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_LASTSAVED), '1900-01-01')) is null and 
                src:EAH_LASTSAVED is not null) THEN
                'EAH_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EAH_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EAH_PK  ORDER BY 
            src:EAH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ENTAPPHEADER as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_APPDATE), '1900-01-01')) is null and 
                    src:EAH_APPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_DATE), '1900-01-01')) is null and 
                    src:EAH_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_LASTSAVED), '1900-01-01')) is null and 
                    src:EAH_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EAH_PK), '0'), 38, 10) is null and 
                    src:EAH_PK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EAH_REVISION), '0'), 38, 10) is null and 
                    src:EAH_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EAH_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EAH_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EAH_LASTSAVED), '1900-01-01')) is null and 
                src:EAH_LASTSAVED is not null)