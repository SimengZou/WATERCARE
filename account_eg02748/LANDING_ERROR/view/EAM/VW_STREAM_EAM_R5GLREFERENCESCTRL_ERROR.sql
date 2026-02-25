CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5GLREFERENCESCTRL_ERROR AS SELECT src, 'EAM_R5GLREFERENCESCTRL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRC_DISPLAYSEQ), '0'), 38, 10) is null and 
                    src:GRC_DISPLAYSEQ is not null) THEN
                    'GRC_DISPLAYSEQ contains a non-numeric value : \'' || AS_VARCHAR(src:GRC_DISPLAYSEQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRC_LASTSAVED), '1900-01-01')) is null and 
                    src:GRC_LASTSAVED is not null) THEN
                    'GRC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GRC_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRC_LENGTH), '0'), 38, 10) is null and 
                    src:GRC_LENGTH is not null) THEN
                    'GRC_LENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:GRC_LENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GRC_UPDATECOUNT is not null) THEN
                    'GRC_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:GRC_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRC_LASTSAVED), '1900-01-01')) is null and 
                src:GRC_LASTSAVED is not null) THEN
                'GRC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GRC_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:GRC_R5COLUMN  ORDER BY 
            src:GRC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GLREFERENCESCTRL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRC_DISPLAYSEQ), '0'), 38, 10) is null and 
                    src:GRC_DISPLAYSEQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRC_LASTSAVED), '1900-01-01')) is null and 
                    src:GRC_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRC_LENGTH), '0'), 38, 10) is null and 
                    src:GRC_LENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GRC_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRC_LASTSAVED), '1900-01-01')) is null and 
                src:GRC_LASTSAVED is not null)