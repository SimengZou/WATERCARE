CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM501_ERROR AS SELECT src, 'LN_TSCTM501' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstp_ref_compnr), '0'), 38, 10) is null and 
                    src:cstp_ref_compnr is not null) THEN
                    'cstp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvln), '0'), 38, 10) is null and 
                    src:cvln is not null) THEN
                    'cvln contains a non-numeric value : \'' || AS_VARCHAR(src:cvln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) THEN
                    'item_sern_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sern_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:litm_lser_ref_compnr), '0'), 38, 10) is null and 
                    src:litm_lser_ref_compnr is not null) THEN
                    'litm_lser_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:litm_lser_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:litm_ref_compnr), '0'), 38, 10) is null and 
                    src:litm_ref_compnr is not null) THEN
                    'litm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:litm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) THEN
                    'ortp contains a non-numeric value : \'' || AS_VARCHAR(src:ortp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_1), '0'), 38, 10) is null and 
                    src:spco_1 is not null) THEN
                    'spco_1 contains a non-numeric value : \'' || AS_VARCHAR(src:spco_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_2), '0'), 38, 10) is null and 
                    src:spco_2 is not null) THEN
                    'spco_2 contains a non-numeric value : \'' || AS_VARCHAR(src:spco_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_3), '0'), 38, 10) is null and 
                    src:spco_3 is not null) THEN
                    'spco_3 contains a non-numeric value : \'' || AS_VARCHAR(src:spco_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_dwhc), '0'), 38, 10) is null and 
                    src:spco_dwhc is not null) THEN
                    'spco_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:spco_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_refc), '0'), 38, 10) is null and 
                    src:spco_refc is not null) THEN
                    'spco_refc contains a non-numeric value : \'' || AS_VARCHAR(src:spco_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_trnc), '0'), 38, 10) is null and 
                    src:spco_trnc is not null) THEN
                    'spco_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:spco_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa), '0'), 38, 10) is null and 
                    src:spsa is not null) THEN
                    'spsa contains a non-numeric value : \'' || AS_VARCHAR(src:spsa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_dwhc), '0'), 38, 10) is null and 
                    src:spsa_dwhc is not null) THEN
                    'spsa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_homc), '0'), 38, 10) is null and 
                    src:spsa_homc is not null) THEN
                    'spsa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_refc), '0'), 38, 10) is null and 
                    src:spsa_refc is not null) THEN
                    'spsa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc1), '0'), 38, 10) is null and 
                    src:spsa_rpc1 is not null) THEN
                    'spsa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc2), '0'), 38, 10) is null and 
                    src:spsa_rpc2 is not null) THEN
                    'spsa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) THEN
                    'tcst contains a non-numeric value : \'' || AS_VARCHAR(src:tcst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trtm), '1900-01-01')) is null and 
                    src:trtm is not null) THEN
                    'trtm contains a non-timestamp value : \'' || AS_VARCHAR(src:trtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrtp), '0'), 38, 10) is null and 
                    src:wrtp is not null) THEN
                    'wrtp contains a non-numeric value : \'' || AS_VARCHAR(src:wrtp) || '\'' WHEN 
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
                                    
                src:sern,
                src:seqn,
                src:trtm,
                src:compnr,
                src:item,
                src:wrty  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM501 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstp_ref_compnr), '0'), 38, 10) is null and 
                    src:cstp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvln), '0'), 38, 10) is null and 
                    src:cvln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:litm_lser_ref_compnr), '0'), 38, 10) is null and 
                    src:litm_lser_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:litm_ref_compnr), '0'), 38, 10) is null and 
                    src:litm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_1), '0'), 38, 10) is null and 
                    src:spco_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_2), '0'), 38, 10) is null and 
                    src:spco_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_3), '0'), 38, 10) is null and 
                    src:spco_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_dwhc), '0'), 38, 10) is null and 
                    src:spco_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_refc), '0'), 38, 10) is null and 
                    src:spco_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_trnc), '0'), 38, 10) is null and 
                    src:spco_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa), '0'), 38, 10) is null and 
                    src:spsa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_dwhc), '0'), 38, 10) is null and 
                    src:spsa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_homc), '0'), 38, 10) is null and 
                    src:spsa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_refc), '0'), 38, 10) is null and 
                    src:spsa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc1), '0'), 38, 10) is null and 
                    src:spsa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc2), '0'), 38, 10) is null and 
                    src:spsa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trtm), '1900-01-01')) is null and 
                    src:trtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrtp), '0'), 38, 10) is null and 
                    src:wrtp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)