CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINA117_ERROR AS SELECT src, 'LN_WHINA117' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) THEN
                    'hour contains a non-numeric value : \'' || AS_VARCHAR(src:hour) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_cwar_trdt_seqn_ref_compnr), '0'), 38, 10) is null and 
                    src:item_cwar_trdt_seqn_ref_compnr is not null) THEN
                    'item_cwar_trdt_seqn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_cwar_trdt_seqn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_1), '0'), 38, 10) is null and 
                    src:namt_1 is not null) THEN
                    'namt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:namt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_2), '0'), 38, 10) is null and 
                    src:namt_2 is not null) THEN
                    'namt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:namt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_3), '0'), 38, 10) is null and 
                    src:namt_3 is not null) THEN
                    'namt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:namt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_1), '0'), 38, 10) is null and 
                    src:oamt_1 is not null) THEN
                    'oamt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_2), '0'), 38, 10) is null and 
                    src:oamt_2 is not null) THEN
                    'oamt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_3), '0'), 38, 10) is null and 
                    src:oamt_3 is not null) THEN
                    'oamt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:oamt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
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
                                    
                src:cpcp,
                src:item,
                src:compnr,
                src:seqn,
                src:cwar,
                src:trdt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINA117 as strm)
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_cwar_trdt_seqn_ref_compnr), '0'), 38, 10) is null and 
                    src:item_cwar_trdt_seqn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_1), '0'), 38, 10) is null and 
                    src:namt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_2), '0'), 38, 10) is null and 
                    src:namt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:namt_3), '0'), 38, 10) is null and 
                    src:namt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_1), '0'), 38, 10) is null and 
                    src:oamt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_2), '0'), 38, 10) is null and 
                    src:oamt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt_3), '0'), 38, 10) is null and 
                    src:oamt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)