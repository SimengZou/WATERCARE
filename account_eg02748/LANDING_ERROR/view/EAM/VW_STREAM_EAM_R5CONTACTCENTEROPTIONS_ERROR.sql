CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CONTACTCENTEROPTIONS_ERROR AS SELECT src, 'EAM_R5CONTACTCENTEROPTIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_KBRESULTSPERPAGE), '0'), 38, 10) is null and 
                    src:COP_KBRESULTSPERPAGE is not null) THEN
                    'COP_KBRESULTSPERPAGE contains a non-numeric value : \'' || AS_VARCHAR(src:COP_KBRESULTSPERPAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COP_LASTSAVED), '1900-01-01')) is null and 
                    src:COP_LASTSAVED is not null) THEN
                    'COP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:COP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_MINPENALTY), '0'), 38, 10) is null and 
                    src:COP_MINPENALTY is not null) THEN
                    'COP_MINPENALTY contains a non-numeric value : \'' || AS_VARCHAR(src:COP_MINPENALTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_RECURRINGREQLOOKBACKDAYS), '0'), 38, 10) is null and 
                    src:COP_RECURRINGREQLOOKBACKDAYS is not null) THEN
                    'COP_RECURRINGREQLOOKBACKDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:COP_RECURRINGREQLOOKBACKDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_TOPTENLOOKBACK), '0'), 38, 10) is null and 
                    src:COP_TOPTENLOOKBACK is not null) THEN
                    'COP_TOPTENLOOKBACK contains a non-numeric value : \'' || AS_VARCHAR(src:COP_TOPTENLOOKBACK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:COP_UPDATECOUNT is not null) THEN
                    'COP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:COP_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COP_UPDATED), '1900-01-01')) is null and 
                    src:COP_UPDATED is not null) THEN
                    'COP_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:COP_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_WOCLOSEDAYS), '0'), 38, 10) is null and 
                    src:COP_WOCLOSEDAYS is not null) THEN
                    'COP_WOCLOSEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:COP_WOCLOSEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_WOOPENDAYS), '0'), 38, 10) is null and 
                    src:COP_WOOPENDAYS is not null) THEN
                    'COP_WOOPENDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:COP_WOOPENDAYS) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COP_LASTSAVED), '1900-01-01')) is null and 
                src:COP_LASTSAVED is not null) THEN
                'COP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:COP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:COP_ORG  ORDER BY 
            src:COP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONTACTCENTEROPTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_KBRESULTSPERPAGE), '0'), 38, 10) is null and 
                    src:COP_KBRESULTSPERPAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COP_LASTSAVED), '1900-01-01')) is null and 
                    src:COP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_MINPENALTY), '0'), 38, 10) is null and 
                    src:COP_MINPENALTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_RECURRINGREQLOOKBACKDAYS), '0'), 38, 10) is null and 
                    src:COP_RECURRINGREQLOOKBACKDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_TOPTENLOOKBACK), '0'), 38, 10) is null and 
                    src:COP_TOPTENLOOKBACK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:COP_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COP_UPDATED), '1900-01-01')) is null and 
                    src:COP_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_WOCLOSEDAYS), '0'), 38, 10) is null and 
                    src:COP_WOCLOSEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COP_WOOPENDAYS), '0'), 38, 10) is null and 
                    src:COP_WOOPENDAYS is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COP_LASTSAVED), '1900-01-01')) is null and 
                src:COP_LASTSAVED is not null)