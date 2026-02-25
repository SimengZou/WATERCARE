CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5IMPORTEXPORTGRPSTATUS_ERROR AS SELECT src, 'EAM_R5IMPORTEXPORTGRPSTATUS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_END), '1900-01-01')) is null and 
                    src:IMG_END is not null) THEN
                    'IMG_END contains a non-timestamp value : \'' || AS_VARCHAR(src:IMG_END) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_LASTSAVED), '1900-01-01')) is null and 
                    src:IMG_LASTSAVED is not null) THEN
                    'IMG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:IMG_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMG_PROCESSORDER), '0'), 38, 10) is null and 
                    src:IMG_PROCESSORDER is not null) THEN
                    'IMG_PROCESSORDER contains a non-numeric value : \'' || AS_VARCHAR(src:IMG_PROCESSORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMG_SESSIONID), '0'), 38, 10) is null and 
                    src:IMG_SESSIONID is not null) THEN
                    'IMG_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:IMG_SESSIONID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_START), '1900-01-01')) is null and 
                    src:IMG_START is not null) THEN
                    'IMG_START contains a non-timestamp value : \'' || AS_VARCHAR(src:IMG_START) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_LASTSAVED), '1900-01-01')) is null and 
                src:IMG_LASTSAVED is not null) THEN
                'IMG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:IMG_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:IMG_GROUP,
                src:IMG_SESSIONID  ORDER BY 
            src:IMG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IMPORTEXPORTGRPSTATUS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_END), '1900-01-01')) is null and 
                    src:IMG_END is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_LASTSAVED), '1900-01-01')) is null and 
                    src:IMG_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMG_PROCESSORDER), '0'), 38, 10) is null and 
                    src:IMG_PROCESSORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMG_SESSIONID), '0'), 38, 10) is null and 
                    src:IMG_SESSIONID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_START), '1900-01-01')) is null and 
                    src:IMG_START is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IMG_LASTSAVED), '1900-01-01')) is null and 
                src:IMG_LASTSAVED is not null)