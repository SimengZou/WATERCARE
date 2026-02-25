CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5QUERYFIELD_ERROR AS SELECT src, 'EAM_R5QUERYFIELD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_COLUMNORDER), '0'), 38, 10) is null and 
                    src:DQF_COLUMNORDER is not null) THEN
                    'DQF_COLUMNORDER contains a non-numeric value : \'' || AS_VARCHAR(src:DQF_COLUMNORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_DDSPYID), '0'), 38, 10) is null and 
                    src:DQF_DDSPYID is not null) THEN
                    'DQF_DDSPYID contains a non-numeric value : \'' || AS_VARCHAR(src:DQF_DDSPYID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_FIELDID), '0'), 38, 10) is null and 
                    src:DQF_FIELDID is not null) THEN
                    'DQF_FIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:DQF_FIELDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DQF_LASTSAVED), '1900-01-01')) is null and 
                    src:DQF_LASTSAVED is not null) THEN
                    'DQF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DQF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DQF_UPDATECOUNT is not null) THEN
                    'DQF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DQF_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DQF_UPDATED), '1900-01-01')) is null and 
                    src:DQF_UPDATED is not null) THEN
                    'DQF_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DQF_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DQF_LASTSAVED), '1900-01-01')) is null and 
                src:DQF_LASTSAVED is not null) THEN
                'DQF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DQF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DQF_DDSPYID,
                src:DQF_FIELDID,
                src:DQF_VIEWTYPE  ORDER BY 
            src:DQF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5QUERYFIELD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_COLUMNORDER), '0'), 38, 10) is null and 
                    src:DQF_COLUMNORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_DDSPYID), '0'), 38, 10) is null and 
                    src:DQF_DDSPYID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_FIELDID), '0'), 38, 10) is null and 
                    src:DQF_FIELDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DQF_LASTSAVED), '1900-01-01')) is null and 
                    src:DQF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DQF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DQF_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DQF_UPDATED), '1900-01-01')) is null and 
                    src:DQF_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DQF_LASTSAVED), '1900-01-01')) is null and 
                src:DQF_LASTSAVED is not null)