CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EVENTS_LASTSTATUSUPDATE_ERROR AS SELECT src, 'EAM_R5EVENTS_LASTSTATUSUPDATE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELU_LASTSAVED), '1900-01-01')) is null and 
                    src:ELU_LASTSAVED is not null) THEN
                    'ELU_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ELU_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELU_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:ELU_LASTSTATUSUPDATE is not null) THEN
                    'ELU_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ELU_LASTSTATUSUPDATE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELU_LASTSAVED), '1900-01-01')) is null and 
                src:ELU_LASTSAVED is not null) THEN
                'ELU_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ELU_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ELU_CODE  ORDER BY 
            src:ELU_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTS_LASTSTATUSUPDATE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELU_LASTSAVED), '1900-01-01')) is null and 
                    src:ELU_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELU_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:ELU_LASTSTATUSUPDATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ELU_LASTSAVED), '1900-01-01')) is null and 
                src:ELU_LASTSAVED is not null)