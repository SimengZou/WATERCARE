CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDIPU001_ERROR AS SELECT src, 'LN_TDIPU001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acci), '0'), 38, 10) is null and 
                    src:acci is not null) THEN
                    'acci contains a non-numeric value : \'' || AS_VARCHAR(src:acci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) THEN
                    'afrb contains a non-numeric value : \'' || AS_VARCHAR(src:afrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buyr_ref_compnr), '0'), 38, 10) is null and 
                    src:buyr_ref_compnr is not null) THEN
                    'buyr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:buyr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casl), '0'), 38, 10) is null and 
                    src:casl is not null) THEN
                    'casl contains a non-numeric value : \'' || AS_VARCHAR(src:casl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cims), '0'), 38, 10) is null and 
                    src:cims is not null) THEN
                    'cims contains a non-numeric value : \'' || AS_VARCHAR(src:cims) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) THEN
                    'cpgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is null and 
                    src:csgp_ref_compnr is not null) THEN
                    'csgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) THEN
                    'ctyo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) THEN
                    'cupp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqi_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqi_ref_compnr is not null) THEN
                    'cuqi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) THEN
                    'cuqp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edco), '0'), 38, 10) is null and 
                    src:edco is not null) THEN
                    'edco contains a non-numeric value : \'' || AS_VARCHAR(src:edco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstd), '0'), 38, 10) is null and 
                    src:hstd is not null) THEN
                    'hstd contains a non-numeric value : \'' || AS_VARCHAR(src:hstd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstq), '0'), 38, 10) is null and 
                    src:hstq is not null) THEN
                    'hstq contains a non-numeric value : \'' || AS_VARCHAR(src:hstq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ispr), '0'), 38, 10) is null and 
                    src:ispr is not null) THEN
                    'ispr contains a non-numeric value : \'' || AS_VARCHAR(src:ispr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) THEN
                    'issp contains a non-numeric value : \'' || AS_VARCHAR(src:issp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mlco), '0'), 38, 10) is null and 
                    src:mlco is not null) THEN
                    'mlco contains a non-numeric value : \'' || AS_VARCHAR(src:mlco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmnf), '0'), 38, 10) is null and 
                    src:mmnf is not null) THEN
                    'mmnf contains a non-numeric value : \'' || AS_VARCHAR(src:mmnf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) THEN
                    'mpnr_cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mpnr_cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpyn), '0'), 38, 10) is null and 
                    src:mpyn is not null) THEN
                    'mpyn contains a non-numeric value : \'' || AS_VARCHAR(src:mpyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prip), '0'), 38, 10) is null and 
                    src:prip is not null) THEN
                    'prip contains a non-numeric value : \'' || AS_VARCHAR(src:prip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) THEN
                    'qual contains a non-numeric value : \'' || AS_VARCHAR(src:qual) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retw), '0'), 38, 10) is null and 
                    src:retw is not null) THEN
                    'retw contains a non-numeric value : \'' || AS_VARCHAR(src:retw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rmss), '0'), 38, 10) is null and 
                    src:rmss is not null) THEN
                    'rmss contains a non-numeric value : \'' || AS_VARCHAR(src:rmss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqms), '0'), 38, 10) is null and 
                    src:rqms is not null) THEN
                    'rqms contains a non-numeric value : \'' || AS_VARCHAR(src:rqms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtdm), '0'), 38, 10) is null and 
                    src:rtdm is not null) THEN
                    'rtdm contains a non-numeric value : \'' || AS_VARCHAR(src:rtdm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtdp), '0'), 38, 10) is null and 
                    src:rtdp is not null) THEN
                    'rtdp contains a non-numeric value : \'' || AS_VARCHAR(src:rtdp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtqm), '0'), 38, 10) is null and 
                    src:rtqm is not null) THEN
                    'rtqm contains a non-numeric value : \'' || AS_VARCHAR(src:rtqm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtqp), '0'), 38, 10) is null and 
                    src:rtqp is not null) THEN
                    'rtqp contains a non-numeric value : \'' || AS_VARCHAR(src:rtqp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpr), '0'), 38, 10) is null and 
                    src:scpr is not null) THEN
                    'scpr contains a non-numeric value : \'' || AS_VARCHAR(src:scpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scus), '0'), 38, 10) is null and 
                    src:scus is not null) THEN
                    'scus contains a non-numeric value : \'' || AS_VARCHAR(src:scus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sopr), '0'), 38, 10) is null and 
                    src:sopr is not null) THEN
                    'sopr contains a non-numeric value : \'' || AS_VARCHAR(src:sopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sops), '0'), 38, 10) is null and 
                    src:sops is not null) THEN
                    'sops contains a non-numeric value : \'' || AS_VARCHAR(src:sops) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco), '0'), 38, 10) is null and 
                    src:spco is not null) THEN
                    'spco contains a non-numeric value : \'' || AS_VARCHAR(src:spco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sspr), '0'), 38, 10) is null and 
                    src:sspr is not null) THEN
                    'sspr contains a non-numeric value : \'' || AS_VARCHAR(src:sspr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) THEN
                    'styp contains a non-numeric value : \'' || AS_VARCHAR(src:styp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suti), '0'), 38, 10) is null and 
                    src:suti is not null) THEN
                    'suti contains a non-numeric value : \'' || AS_VARCHAR(src:suti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutu), '0'), 38, 10) is null and 
                    src:sutu is not null) THEN
                    'sutu contains a non-numeric value : \'' || AS_VARCHAR(src:sutu) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp), '0'), 38, 10) is null and 
                    src:txtp is not null) THEN
                    'txtp contains a non-numeric value : \'' || AS_VARCHAR(src:txtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp_ref_compnr), '0'), 38, 10) is null and 
                    src:txtp_ref_compnr is not null) THEN
                    'txtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) THEN
                    'vryn contains a non-numeric value : \'' || AS_VARCHAR(src:vryn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtbo), '0'), 38, 10) is null and 
                    src:vtbo is not null) THEN
                    'vtbo contains a non-numeric value : \'' || AS_VARCHAR(src:vtbo) || '\'' WHEN 
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
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDIPU001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acci), '0'), 38, 10) is null and 
                    src:acci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buyr_ref_compnr), '0'), 38, 10) is null and 
                    src:buyr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casl), '0'), 38, 10) is null and 
                    src:casl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cims), '0'), 38, 10) is null and 
                    src:cims is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is null and 
                    src:csgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqi_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edco), '0'), 38, 10) is null and 
                    src:edco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstd), '0'), 38, 10) is null and 
                    src:hstd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hstq), '0'), 38, 10) is null and 
                    src:hstq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ispr), '0'), 38, 10) is null and 
                    src:ispr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mlco), '0'), 38, 10) is null and 
                    src:mlco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmnf), '0'), 38, 10) is null and 
                    src:mmnf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpyn), '0'), 38, 10) is null and 
                    src:mpyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prip), '0'), 38, 10) is null and 
                    src:prip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retw), '0'), 38, 10) is null and 
                    src:retw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rmss), '0'), 38, 10) is null and 
                    src:rmss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqms), '0'), 38, 10) is null and 
                    src:rqms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtdm), '0'), 38, 10) is null and 
                    src:rtdm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtdp), '0'), 38, 10) is null and 
                    src:rtdp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtqm), '0'), 38, 10) is null and 
                    src:rtqm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtqp), '0'), 38, 10) is null and 
                    src:rtqp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpr), '0'), 38, 10) is null and 
                    src:scpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scus), '0'), 38, 10) is null and 
                    src:scus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sopr), '0'), 38, 10) is null and 
                    src:sopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sops), '0'), 38, 10) is null and 
                    src:sops is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco), '0'), 38, 10) is null and 
                    src:spco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sspr), '0'), 38, 10) is null and 
                    src:sspr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suti), '0'), 38, 10) is null and 
                    src:suti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutu), '0'), 38, 10) is null and 
                    src:sutu is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp), '0'), 38, 10) is null and 
                    src:txtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp_ref_compnr), '0'), 38, 10) is null and 
                    src:txtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtbo), '0'), 38, 10) is null and 
                    src:vtbo is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)