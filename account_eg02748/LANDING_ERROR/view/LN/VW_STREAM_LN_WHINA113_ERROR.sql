CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINA113_ERROR AS SELECT src, 'LN_WHINA113' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_1), '0'), 38, 10) is null and 
                    src:amac_1 is not null) THEN
                    'amac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_2), '0'), 38, 10) is null and 
                    src:amac_2 is not null) THEN
                    'amac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_3), '0'), 38, 10) is null and 
                    src:amac_3 is not null) THEN
                    'amac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amah), '0'), 38, 10) is null and 
                    src:amah is not null) THEN
                    'amah contains a non-numeric value : \'' || AS_VARCHAR(src:amah) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) THEN
                    'amnt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) THEN
                    'amnt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) THEN
                    'amnt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmc_1), '0'), 38, 10) is null and 
                    src:atmc_1 is not null) THEN
                    'atmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:atmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmc_2), '0'), 38, 10) is null and 
                    src:atmc_2 is not null) THEN
                    'atmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:atmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmc_3), '0'), 38, 10) is null and 
                    src:atmc_3 is not null) THEN
                    'atmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:atmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmh), '0'), 38, 10) is null and 
                    src:atmh is not null) THEN
                    'atmh contains a non-numeric value : \'' || AS_VARCHAR(src:atmh) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inwp), '0'), 38, 10) is null and 
                    src:inwp is not null) THEN
                    'inwp contains a non-numeric value : \'' || AS_VARCHAR(src:inwp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_cwar_trdt_seqn_inwp_ref_compnr), '0'), 38, 10) is null and 
                    src:item_cwar_trdt_seqn_inwp_ref_compnr is not null) THEN
                    'item_cwar_trdt_seqn_inwp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_cwar_trdt_seqn_inwp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matc_1), '0'), 38, 10) is null and 
                    src:matc_1 is not null) THEN
                    'matc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:matc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matc_2), '0'), 38, 10) is null and 
                    src:matc_2 is not null) THEN
                    'matc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:matc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matc_3), '0'), 38, 10) is null and 
                    src:matc_3 is not null) THEN
                    'matc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:matc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:math), '0'), 38, 10) is null and 
                    src:math is not null) THEN
                    'math contains a non-numeric value : \'' || AS_VARCHAR(src:math) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc_1), '0'), 38, 10) is null and 
                    src:mauc_1 is not null) THEN
                    'mauc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:mauc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc_2), '0'), 38, 10) is null and 
                    src:mauc_2 is not null) THEN
                    'mauc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:mauc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc_3), '0'), 38, 10) is null and 
                    src:mauc_3 is not null) THEN
                    'mauc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:mauc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauh), '0'), 38, 10) is null and 
                    src:mauh is not null) THEN
                    'mauh contains a non-numeric value : \'' || AS_VARCHAR(src:mauh) || '\'' WHEN 
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
                                    
                src:compnr,
                src:cpcp,
                src:cwar,
                src:trdt,
                src:inwp,
                src:item,
                src:seqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINA113 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_1), '0'), 38, 10) is null and 
                    src:amac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_2), '0'), 38, 10) is null and 
                    src:amac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_3), '0'), 38, 10) is null and 
                    src:amac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amah), '0'), 38, 10) is null and 
                    src:amah is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmc_1), '0'), 38, 10) is null and 
                    src:atmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmc_2), '0'), 38, 10) is null and 
                    src:atmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmc_3), '0'), 38, 10) is null and 
                    src:atmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atmh), '0'), 38, 10) is null and 
                    src:atmh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inwp), '0'), 38, 10) is null and 
                    src:inwp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_cwar_trdt_seqn_inwp_ref_compnr), '0'), 38, 10) is null and 
                    src:item_cwar_trdt_seqn_inwp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matc_1), '0'), 38, 10) is null and 
                    src:matc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matc_2), '0'), 38, 10) is null and 
                    src:matc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matc_3), '0'), 38, 10) is null and 
                    src:matc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:math), '0'), 38, 10) is null and 
                    src:math is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc_1), '0'), 38, 10) is null and 
                    src:mauc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc_2), '0'), 38, 10) is null and 
                    src:mauc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauc_3), '0'), 38, 10) is null and 
                    src:mauc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mauh), '0'), 38, 10) is null and 
                    src:mauh is not null) or 
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