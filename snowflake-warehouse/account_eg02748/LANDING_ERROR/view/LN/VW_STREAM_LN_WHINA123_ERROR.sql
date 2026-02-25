CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINA123_ERROR AS SELECT src, 'LN_WHINA123' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) THEN
                    'amnt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) THEN
                    'amnt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) THEN
                    'amnt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) THEN
                    'cpcp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) THEN
                    'hour contains a non-numeric value : \'' || AS_VARCHAR(src:hour) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) THEN
                    'rorg contains a non-numeric value : \'' || AS_VARCHAR(src:rorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg_orno_seqn_ref_compnr), '0'), 38, 10) is null and 
                    src:rorg_orno_seqn_ref_compnr is not null) THEN
                    'rorg_orno_seqn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rorg_orno_seqn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
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
                                    
                src:rorg,
                src:orno,
                src:seqn,
                src:cpcp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINA123 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg_orno_seqn_ref_compnr), '0'), 38, 10) is null and 
                    src:rorg_orno_seqn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)