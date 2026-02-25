CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5GRIDFIELD_ERROR AS SELECT src, 'EAM_R5GRIDFIELD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_BOTNUMBER), '0'), 38, 10) is null and 
                    src:GFD_BOTNUMBER is not null) THEN
                    'GFD_BOTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:GFD_BOTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_DEPENDENT), '0'), 38, 10) is null and 
                    src:GFD_DEPENDENT is not null) THEN
                    'GFD_DEPENDENT contains a non-numeric value : \'' || AS_VARCHAR(src:GFD_DEPENDENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_FIELDID), '0'), 38, 10) is null and 
                    src:GFD_FIELDID is not null) THEN
                    'GFD_FIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:GFD_FIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_GRIDID), '0'), 38, 10) is null and 
                    src:GFD_GRIDID is not null) THEN
                    'GFD_GRIDID contains a non-numeric value : \'' || AS_VARCHAR(src:GFD_GRIDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GFD_LASTSAVED), '1900-01-01')) is null and 
                    src:GFD_LASTSAVED is not null) THEN
                    'GFD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GFD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_OCCURRENCE), '0'), 38, 10) is null and 
                    src:GFD_OCCURRENCE is not null) THEN
                    'GFD_OCCURRENCE contains a non-numeric value : \'' || AS_VARCHAR(src:GFD_OCCURRENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GFD_UPDATECOUNT is not null) THEN
                    'GFD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:GFD_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GFD_UPDATED), '1900-01-01')) is null and 
                    src:GFD_UPDATED is not null) THEN
                    'GFD_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:GFD_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GFD_LASTSAVED), '1900-01-01')) is null and 
                src:GFD_LASTSAVED is not null) THEN
                'GFD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GFD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:GFD_FIELDID,
                src:GFD_GRIDID,
                src:GFD_OCCURRENCE  ORDER BY 
            src:GFD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GRIDFIELD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_BOTNUMBER), '0'), 38, 10) is null and 
                    src:GFD_BOTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_DEPENDENT), '0'), 38, 10) is null and 
                    src:GFD_DEPENDENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_FIELDID), '0'), 38, 10) is null and 
                    src:GFD_FIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_GRIDID), '0'), 38, 10) is null and 
                    src:GFD_GRIDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GFD_LASTSAVED), '1900-01-01')) is null and 
                    src:GFD_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_OCCURRENCE), '0'), 38, 10) is null and 
                    src:GFD_OCCURRENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GFD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GFD_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GFD_UPDATED), '1900-01-01')) is null and 
                    src:GFD_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GFD_LASTSAVED), '1900-01-01')) is null and 
                src:GFD_LASTSAVED is not null)