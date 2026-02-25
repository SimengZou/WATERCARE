CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC354_ERROR AS SELECT src, 'LN_TPPTC354' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_1), '0'), 38, 10) is null and 
                    src:amch_1 is not null) THEN
                    'amch_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amch_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_2), '0'), 38, 10) is null and 
                    src:amch_2 is not null) THEN
                    'amch_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amch_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_3), '0'), 38, 10) is null and 
                    src:amch_3 is not null) THEN
                    'amch_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amch_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_4), '0'), 38, 10) is null and 
                    src:amch_4 is not null) THEN
                    'amch_4 contains a non-numeric value : \'' || AS_VARCHAR(src:amch_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_1), '0'), 38, 10) is null and 
                    src:ames_1 is not null) THEN
                    'ames_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ames_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_2), '0'), 38, 10) is null and 
                    src:ames_2 is not null) THEN
                    'ames_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ames_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_3), '0'), 38, 10) is null and 
                    src:ames_3 is not null) THEN
                    'ames_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ames_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_4), '0'), 38, 10) is null and 
                    src:ames_4 is not null) THEN
                    'ames_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ames_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) THEN
                    'cprj_ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null) THEN
                'sequencenumber contains a non-timestamp value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:compnr,
                src:ccic,
                src:ccal,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC354 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_1), '0'), 38, 10) is null and 
                    src:amch_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_2), '0'), 38, 10) is null and 
                    src:amch_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_3), '0'), 38, 10) is null and 
                    src:amch_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch_4), '0'), 38, 10) is null and 
                    src:amch_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_1), '0'), 38, 10) is null and 
                    src:ames_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_2), '0'), 38, 10) is null and 
                    src:ames_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_3), '0'), 38, 10) is null and 
                    src:ames_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ames_4), '0'), 38, 10) is null and 
                    src:ames_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)