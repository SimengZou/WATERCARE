CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ALERTIONPULSE_ERROR AS SELECT src, 'EAM_R5ALERTIONPULSE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_DELAY), '0'), 38, 10) is null and 
                    src:ALI_DELAY is not null) THEN
                    'ALI_DELAY contains a non-numeric value : \'' || AS_VARCHAR(src:ALI_DELAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_DESCRIPTIONFIELDID), '0'), 38, 10) is null and 
                    src:ALI_DESCRIPTIONFIELDID is not null) THEN
                    'ALI_DESCRIPTIONFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALI_DESCRIPTIONFIELDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALI_LASTSAVED), '1900-01-01')) is null and 
                    src:ALI_LASTSAVED is not null) THEN
                    'ALI_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALI_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_RECIPIENTFIELDID), '0'), 38, 10) is null and 
                    src:ALI_RECIPIENTFIELDID is not null) THEN
                    'ALI_RECIPIENTFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALI_RECIPIENTFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ALI_UPDATECOUNT is not null) THEN
                    'ALI_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ALI_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALI_LASTSAVED), '1900-01-01')) is null and 
                src:ALI_LASTSAVED is not null) THEN
                'ALI_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALI_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ALI_ALERT  ORDER BY 
            src:ALI_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTIONPULSE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_DELAY), '0'), 38, 10) is null and 
                    src:ALI_DELAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_DESCRIPTIONFIELDID), '0'), 38, 10) is null and 
                    src:ALI_DESCRIPTIONFIELDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALI_LASTSAVED), '1900-01-01')) is null and 
                    src:ALI_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_RECIPIENTFIELDID), '0'), 38, 10) is null and 
                    src:ALI_RECIPIENTFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALI_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ALI_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALI_LASTSAVED), '1900-01-01')) is null and 
                src:ALI_LASTSAVED is not null)