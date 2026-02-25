CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CONPARTS_ERROR AS SELECT src, 'EAM_R5CONPARTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CPA_LASTSAVED), '1900-01-01')) is null and 
                    src:CPA_LASTSAVED is not null) THEN
                    'CPA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CPA_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_LEADTIME), '0'), 38, 10) is null and 
                    src:CPA_LEADTIME is not null) THEN
                    'CPA_LEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:CPA_LEADTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_MULTIPLY), '0'), 38, 10) is null and 
                    src:CPA_MULTIPLY is not null) THEN
                    'CPA_MULTIPLY contains a non-numeric value : \'' || AS_VARCHAR(src:CPA_MULTIPLY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_PRICE), '0'), 38, 10) is null and 
                    src:CPA_PRICE is not null) THEN
                    'CPA_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:CPA_PRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CPA_UPDATECOUNT is not null) THEN
                    'CPA_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CPA_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CPA_LASTSAVED), '1900-01-01')) is null and 
                src:CPA_LASTSAVED is not null) THEN
                'CPA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CPA_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CPA_CONTRACT,
                src:CPA_PART,
                src:CPA_PART_ORG  ORDER BY 
            src:CPA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONPARTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CPA_LASTSAVED), '1900-01-01')) is null and 
                    src:CPA_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_LEADTIME), '0'), 38, 10) is null and 
                    src:CPA_LEADTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_MULTIPLY), '0'), 38, 10) is null and 
                    src:CPA_MULTIPLY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_PRICE), '0'), 38, 10) is null and 
                    src:CPA_PRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CPA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CPA_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CPA_LASTSAVED), '1900-01-01')) is null and 
                src:CPA_LASTSAVED is not null)