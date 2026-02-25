CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5RELIABILITYRANKINGLEVELS_ERROR AS SELECT src, 'EAM_R5RELIABILITYRANKINGLEVELS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRL_LASTSAVED), '1900-01-01')) is null and 
                    src:RRL_LASTSAVED is not null) THEN
                    'RRL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_MAX), '0'), 38, 10) is null and 
                    src:RRL_MAX is not null) THEN
                    'RRL_MAX contains a non-numeric value : \'' || AS_VARCHAR(src:RRL_MAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_MIN), '0'), 38, 10) is null and 
                    src:RRL_MIN is not null) THEN
                    'RRL_MIN contains a non-numeric value : \'' || AS_VARCHAR(src:RRL_MIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_REVISION), '0'), 38, 10) is null and 
                    src:RRL_REVISION is not null) THEN
                    'RRL_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:RRL_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_SEQ), '0'), 38, 10) is null and 
                    src:RRL_SEQ is not null) THEN
                    'RRL_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:RRL_SEQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RRL_UPDATECOUNT is not null) THEN
                    'RRL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:RRL_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRL_LASTSAVED), '1900-01-01')) is null and 
                src:RRL_LASTSAVED is not null) THEN
                'RRL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:RRL_PK  ORDER BY 
            src:RRL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGLEVELS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRL_LASTSAVED), '1900-01-01')) is null and 
                    src:RRL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_MAX), '0'), 38, 10) is null and 
                    src:RRL_MAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_MIN), '0'), 38, 10) is null and 
                    src:RRL_MIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_REVISION), '0'), 38, 10) is null and 
                    src:RRL_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_SEQ), '0'), 38, 10) is null and 
                    src:RRL_SEQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RRL_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRL_LASTSAVED), '1900-01-01')) is null and 
                src:RRL_LASTSAVED is not null)