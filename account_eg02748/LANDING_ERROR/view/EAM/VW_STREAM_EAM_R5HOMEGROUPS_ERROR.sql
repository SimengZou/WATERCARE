CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5HOMEGROUPS_ERROR AS SELECT src, 'EAM_R5HOMEGROUPS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HMG_LASTSAVED), '1900-01-01')) is null and 
                    src:HMG_LASTSAVED is not null) THEN
                    'HMG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:HMG_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HMG_SEQ), '0'), 38, 10) is null and 
                    src:HMG_SEQ is not null) THEN
                    'HMG_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:HMG_SEQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HMG_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:HMG_UPDATECOUNT is not null) THEN
                    'HMG_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:HMG_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HMG_LASTSAVED), '1900-01-01')) is null and 
                src:HMG_LASTSAVED is not null) THEN
                'HMG_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:HMG_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:HMG_GROUP,
                src:HMG_HOMCODE,
                src:HMG_HOMTYPE  ORDER BY 
            src:HMG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5HOMEGROUPS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HMG_LASTSAVED), '1900-01-01')) is null and 
                    src:HMG_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HMG_SEQ), '0'), 38, 10) is null and 
                    src:HMG_SEQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HMG_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:HMG_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HMG_LASTSAVED), '1900-01-01')) is null and 
                src:HMG_LASTSAVED is not null)