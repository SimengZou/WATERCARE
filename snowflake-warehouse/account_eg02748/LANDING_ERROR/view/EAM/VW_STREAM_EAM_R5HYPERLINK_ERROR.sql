CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5HYPERLINK_ERROR AS SELECT src, 'EAM_R5HYPERLINK' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_DATASPY), '0'), 38, 10) is null and 
                    src:HYP_DATASPY is not null) THEN
                    'HYP_DATASPY contains a non-numeric value : \'' || AS_VARCHAR(src:HYP_DATASPY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HYP_LASTSAVED), '1900-01-01')) is null and 
                    src:HYP_LASTSAVED is not null) THEN
                    'HYP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:HYP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_PK), '0'), 38, 10) is null and 
                    src:HYP_PK is not null) THEN
                    'HYP_PK contains a non-numeric value : \'' || AS_VARCHAR(src:HYP_PK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_SRCLINENUMBER), '0'), 38, 10) is null and 
                    src:HYP_SRCLINENUMBER is not null) THEN
                    'HYP_SRCLINENUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:HYP_SRCLINENUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:HYP_UPDATECOUNT is not null) THEN
                    'HYP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:HYP_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HYP_LASTSAVED), '1900-01-01')) is null and 
                src:HYP_LASTSAVED is not null) THEN
                'HYP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:HYP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:HYP_PK  ORDER BY 
            src:HYP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5HYPERLINK as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_DATASPY), '0'), 38, 10) is null and 
                    src:HYP_DATASPY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HYP_LASTSAVED), '1900-01-01')) is null and 
                    src:HYP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_PK), '0'), 38, 10) is null and 
                    src:HYP_PK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_SRCLINENUMBER), '0'), 38, 10) is null and 
                    src:HYP_SRCLINENUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HYP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:HYP_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HYP_LASTSAVED), '1900-01-01')) is null and 
                src:HYP_LASTSAVED is not null)