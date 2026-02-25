CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PROMPTWEBSERVICES_ERROR AS SELECT src, 'EAM_R5PROMPTWEBSERVICES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PWS_LASTSAVED), '1900-01-01')) is null and 
                    src:PWS_LASTSAVED is not null) THEN
                    'PWS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PWS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PWS_PROCESSGROUP), '0'), 38, 10) is null and 
                    src:PWS_PROCESSGROUP is not null) THEN
                    'PWS_PROCESSGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:PWS_PROCESSGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PWS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PWS_UPDATECOUNT is not null) THEN
                    'PWS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PWS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PWS_UPDATED), '1900-01-01')) is null and 
                    src:PWS_UPDATED is not null) THEN
                    'PWS_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PWS_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PWS_LASTSAVED), '1900-01-01')) is null and 
                src:PWS_LASTSAVED is not null) THEN
                'PWS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PWS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PWS_PROCESSGROUP,
                src:PWS_WSPRMCODE  ORDER BY 
            src:PWS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROMPTWEBSERVICES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PWS_LASTSAVED), '1900-01-01')) is null and 
                    src:PWS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PWS_PROCESSGROUP), '0'), 38, 10) is null and 
                    src:PWS_PROCESSGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PWS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PWS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PWS_UPDATED), '1900-01-01')) is null and 
                    src:PWS_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PWS_LASTSAVED), '1900-01-01')) is null and 
                src:PWS_LASTSAVED is not null)