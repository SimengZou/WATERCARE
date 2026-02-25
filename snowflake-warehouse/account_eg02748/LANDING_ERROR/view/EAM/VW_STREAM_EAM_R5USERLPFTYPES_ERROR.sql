CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5USERLPFTYPES_ERROR AS SELECT src, 'EAM_R5USERLPFTYPES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LPT_LASTSAVED), '1900-01-01')) is null and 
                    src:LPT_LASTSAVED is not null) THEN
                    'LPT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LPT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LPT_SEQUENCE), '0'), 38, 10) is null and 
                    src:LPT_SEQUENCE is not null) THEN
                    'LPT_SEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:LPT_SEQUENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LPT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:LPT_UPDATECOUNT is not null) THEN
                    'LPT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:LPT_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LPT_LASTSAVED), '1900-01-01')) is null and 
                src:LPT_LASTSAVED is not null) THEN
                'LPT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LPT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:LPT_LINEARPREFERENCE,
                src:LPT_TYPE  ORDER BY 
            src:LPT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERLPFTYPES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LPT_LASTSAVED), '1900-01-01')) is null and 
                    src:LPT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LPT_SEQUENCE), '0'), 38, 10) is null and 
                    src:LPT_SEQUENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LPT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:LPT_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LPT_LASTSAVED), '1900-01-01')) is null and 
                src:LPT_LASTSAVED is not null)