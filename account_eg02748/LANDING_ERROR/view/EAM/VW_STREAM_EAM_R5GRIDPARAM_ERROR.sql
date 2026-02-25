CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5GRIDPARAM_ERROR AS SELECT src, 'EAM_R5GRIDPARAM' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GDP_GRIDID), '0'), 38, 10) is null and 
                    src:GDP_GRIDID is not null) THEN
                    'GDP_GRIDID contains a non-numeric value : \'' || AS_VARCHAR(src:GDP_GRIDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GDP_LASTSAVED), '1900-01-01')) is null and 
                    src:GDP_LASTSAVED is not null) THEN
                    'GDP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GDP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GDP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GDP_UPDATECOUNT is not null) THEN
                    'GDP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:GDP_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GDP_UPDATED), '1900-01-01')) is null and 
                    src:GDP_UPDATED is not null) THEN
                    'GDP_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:GDP_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GDP_LASTSAVED), '1900-01-01')) is null and 
                src:GDP_LASTSAVED is not null) THEN
                'GDP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GDP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:GDP_GRIDID,
                src:GDP_PARAM  ORDER BY 
            src:GDP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GRIDPARAM as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GDP_GRIDID), '0'), 38, 10) is null and 
                    src:GDP_GRIDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GDP_LASTSAVED), '1900-01-01')) is null and 
                    src:GDP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GDP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GDP_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GDP_UPDATED), '1900-01-01')) is null and 
                    src:GDP_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GDP_LASTSAVED), '1900-01-01')) is null and 
                src:GDP_LASTSAVED is not null)