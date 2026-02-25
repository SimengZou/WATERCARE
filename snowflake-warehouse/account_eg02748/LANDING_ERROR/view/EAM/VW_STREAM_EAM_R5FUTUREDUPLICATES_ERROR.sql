CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5FUTUREDUPLICATES_ERROR AS SELECT src, 'EAM_R5FUTUREDUPLICATES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FDP_DATE), '1900-01-01')) is null and 
                    src:FDP_DATE is not null) THEN
                    'FDP_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:FDP_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FDP_LASTSAVED), '1900-01-01')) is null and 
                    src:FDP_LASTSAVED is not null) THEN
                    'FDP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:FDP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FDP_PPOPK), '0'), 38, 10) is null and 
                    src:FDP_PPOPK is not null) THEN
                    'FDP_PPOPK contains a non-numeric value : \'' || AS_VARCHAR(src:FDP_PPOPK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FDP_SEQNO), '0'), 38, 10) is null and 
                    src:FDP_SEQNO is not null) THEN
                    'FDP_SEQNO contains a non-numeric value : \'' || AS_VARCHAR(src:FDP_SEQNO) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FDP_LASTSAVED), '1900-01-01')) is null and 
                src:FDP_LASTSAVED is not null) THEN
                'FDP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:FDP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:FDP_PPOPK,
                src:FDP_SEQNO  ORDER BY 
            src:FDP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FUTUREDUPLICATES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FDP_DATE), '1900-01-01')) is null and 
                    src:FDP_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FDP_LASTSAVED), '1900-01-01')) is null and 
                    src:FDP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FDP_PPOPK), '0'), 38, 10) is null and 
                    src:FDP_PPOPK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FDP_SEQNO), '0'), 38, 10) is null and 
                    src:FDP_SEQNO is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FDP_LASTSAVED), '1900-01-01')) is null and 
                src:FDP_LASTSAVED is not null)