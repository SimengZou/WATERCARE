CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH501_ERROR AS SELECT src, 'LN_WHINH501' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpr), '0'), 38, 10) is null and 
                    src:adpr is not null) THEN
                    'adpr contains a non-numeric value : \'' || AS_VARCHAR(src:adpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) THEN
                    'appr contains a non-numeric value : \'' || AS_VARCHAR(src:appr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadj_ref_compnr), '0'), 38, 10) is null and 
                    src:cadj_ref_compnr is not null) THEN
                    'cadj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) THEN
                    'cdat contains a non-timestamp value : \'' || AS_VARCHAR(src:cdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cntn), '0'), 38, 10) is null and 
                    src:cntn is not null) THEN
                    'cntn contains a non-numeric value : \'' || AS_VARCHAR(src:cntn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) THEN
                    'csts contains a non-numeric value : \'' || AS_VARCHAR(src:csts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_loca_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_loca_ref_compnr is not null) THEN
                    'cwar_loca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_loca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpby), '0'), 38, 10) is null and 
                    src:dpby is not null) THEN
                    'dpby contains a non-numeric value : \'' || AS_VARCHAR(src:dpby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) THEN
                    'huid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:huid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hupr), '0'), 38, 10) is null and 
                    src:hupr is not null) THEN
                    'hupr contains a non-numeric value : \'' || AS_VARCHAR(src:hupr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:idat), '1900-01-01')) is null and 
                    src:idat is not null) THEN
                    'idat contains a non-timestamp value : \'' || AS_VARCHAR(src:idat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iown), '0'), 38, 10) is null and 
                    src:iown is not null) THEN
                    'iown contains a non-numeric value : \'' || AS_VARCHAR(src:iown) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:istr), '0'), 38, 10) is null and 
                    src:istr is not null) THEN
                    'istr contains a non-numeric value : \'' || AS_VARCHAR(src:istr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) THEN
                    'item_clot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_clot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nstp), '0'), 38, 10) is null and 
                    src:nstp is not null) THEN
                    'nstp contains a non-numeric value : \'' || AS_VARCHAR(src:nstp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cntn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cntn_ref_compnr is not null) THEN
                    'orno_cntn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_cntn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) THEN
                    'ownr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ownr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) THEN
                    'pkdf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pkdf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcd), '0'), 38, 10) is null and 
                    src:prcd is not null) THEN
                    'prcd contains a non-numeric value : \'' || AS_VARCHAR(src:prcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnr), '0'), 38, 10) is null and 
                    src:qcnr is not null) THEN
                    'qcnr contains a non-numeric value : \'' || AS_VARCHAR(src:qcnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnt), '0'), 38, 10) is null and 
                    src:qcnt is not null) THEN
                    'qcnt contains a non-numeric value : \'' || AS_VARCHAR(src:qcnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcpu), '0'), 38, 10) is null and 
                    src:qcpu is not null) THEN
                    'qcpu contains a non-numeric value : \'' || AS_VARCHAR(src:qcpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspu), '0'), 38, 10) is null and 
                    src:qspu is not null) THEN
                    'qspu contains a non-numeric value : \'' || AS_VARCHAR(src:qspu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstp), '0'), 38, 10) is null and 
                    src:qstp is not null) THEN
                    'qstp contains a non-numeric value : \'' || AS_VARCHAR(src:qstp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstr), '0'), 38, 10) is null and 
                    src:qstr is not null) THEN
                    'qstr contains a non-numeric value : \'' || AS_VARCHAR(src:qstr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qvpu), '0'), 38, 10) is null and 
                    src:qvpu is not null) THEN
                    'qvpu contains a non-numeric value : \'' || AS_VARCHAR(src:qvpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qvrc), '0'), 38, 10) is null and 
                    src:qvrc is not null) THEN
                    'qvrc contains a non-numeric value : \'' || AS_VARCHAR(src:qvrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qvrr), '0'), 38, 10) is null and 
                    src:qvrr is not null) THEN
                    'qvrr contains a non-numeric value : \'' || AS_VARCHAR(src:qvrr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) THEN
                    'reco contains a non-numeric value : \'' || AS_VARCHAR(src:reco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stun_ref_compnr), '0'), 38, 10) is null and 
                    src:stun_ref_compnr is not null) THEN
                    'stun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uapr), '0'), 38, 10) is null and 
                    src:uapr is not null) THEN
                    'uapr contains a non-numeric value : \'' || AS_VARCHAR(src:uapr) || '\'' WHEN 
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
                                    
                src:pono,
                src:compnr,
                src:orno,
                src:cntn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH501 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpr), '0'), 38, 10) is null and 
                    src:adpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadj_ref_compnr), '0'), 38, 10) is null and 
                    src:cadj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cntn), '0'), 38, 10) is null and 
                    src:cntn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_loca_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_loca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpby), '0'), 38, 10) is null and 
                    src:dpby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hupr), '0'), 38, 10) is null and 
                    src:hupr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:idat), '1900-01-01')) is null and 
                    src:idat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iown), '0'), 38, 10) is null and 
                    src:iown is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:istr), '0'), 38, 10) is null and 
                    src:istr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nstp), '0'), 38, 10) is null and 
                    src:nstp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_cntn_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_cntn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcd), '0'), 38, 10) is null and 
                    src:prcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnr), '0'), 38, 10) is null and 
                    src:qcnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcnt), '0'), 38, 10) is null and 
                    src:qcnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcpu), '0'), 38, 10) is null and 
                    src:qcpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspu), '0'), 38, 10) is null and 
                    src:qspu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstp), '0'), 38, 10) is null and 
                    src:qstp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstr), '0'), 38, 10) is null and 
                    src:qstr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qvpu), '0'), 38, 10) is null and 
                    src:qvpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qvrc), '0'), 38, 10) is null and 
                    src:qvrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qvrr), '0'), 38, 10) is null and 
                    src:qvrr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stun_ref_compnr), '0'), 38, 10) is null and 
                    src:stun_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uapr), '0'), 38, 10) is null and 
                    src:uapr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)