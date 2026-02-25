CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR012_ERROR AS SELECT src, 'LN_TDPUR012' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngco_ref_compnr), '0'), 38, 10) is null and 
                    src:ngco_ref_compnr is not null) THEN
                    'ngco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngco_scid_ref_compnr), '0'), 38, 10) is null and 
                    src:ngco_scid_ref_compnr is not null) THEN
                    'ngco_scid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngco_scid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_ref_compnr is not null) THEN
                    'ngpc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_sepc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_sepc_ref_compnr is not null) THEN
                    'ngpc_sepc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpc_sepc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_ref_compnr is not null) THEN
                    'ngpo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_sepo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_sepo_ref_compnr is not null) THEN
                    'ngpo_sepo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpo_sepo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_seps_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_seps_ref_compnr is not null) THEN
                    'ngpo_seps_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpo_seps_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_ref_compnr is not null) THEN
                    'ngpq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_sepq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_sepq_ref_compnr is not null) THEN
                    'ngpq_sepq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpq_sepq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpr_ref_compnr is not null) THEN
                    'ngpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpr_sepr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpr_sepr_ref_compnr is not null) THEN
                    'ngpr_sepr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpr_sepr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrl_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrl_ref_compnr is not null) THEN
                    'ngrl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrl_serl_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrl_serl_ref_compnr is not null) THEN
                    'ngrl_serl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrl_serl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_ref_compnr is not null) THEN
                    'ngsp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_safp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_safp_ref_compnr is not null) THEN
                    'ngsp_safp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsp_safp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_seqo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_seqo_ref_compnr is not null) THEN
                    'ngsp_seqo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsp_seqo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_sequ_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_sequ_ref_compnr is not null) THEN
                    'ngsp_sequ_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsp_sequ_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_sspo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_sspo_ref_compnr is not null) THEN
                    'ngsp_sspo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsp_sspo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
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
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR012 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngco_ref_compnr), '0'), 38, 10) is null and 
                    src:ngco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngco_scid_ref_compnr), '0'), 38, 10) is null and 
                    src:ngco_scid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_sepc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_sepc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_sepo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_sepo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_seps_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_seps_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_sepq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_sepq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpr_sepr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpr_sepr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrl_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrl_serl_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrl_serl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_safp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_safp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_seqo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_seqo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_sequ_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_sequ_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsp_sspo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsp_sspo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)