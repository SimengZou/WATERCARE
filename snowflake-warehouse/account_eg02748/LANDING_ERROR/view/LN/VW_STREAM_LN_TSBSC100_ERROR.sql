CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSBSC100_ERROR AS SELECT src, 'LN_TSBSC100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfad_ref_compnr), '0'), 38, 10) is null and 
                    src:bfad_ref_compnr is not null) THEN
                    'bfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) THEN
                    'bfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:bfcn_ref_compnr is not null) THEN
                    'bfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccha), '0'), 38, 10) is null and 
                    src:ccha is not null) THEN
                    'ccha contains a non-numeric value : \'' || AS_VARCHAR(src:ccha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) THEN
                    'clrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) THEN
                    'csar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cusc_ref_compnr), '0'), 38, 10) is null and 
                    src:cusc_ref_compnr is not null) THEN
                    'cusc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cusc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:czon_ref_compnr), '0'), 38, 10) is null and 
                    src:czon_ref_compnr is not null) THEN
                    'czon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:czon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbpo), '0'), 38, 10) is null and 
                    src:dbpo is not null) THEN
                    'dbpo contains a non-numeric value : \'' || AS_VARCHAR(src:dbpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlad_ref_compnr), '0'), 38, 10) is null and 
                    src:dlad_ref_compnr is not null) THEN
                    'dlad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dlad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlcn_ref_compnr), '0'), 38, 10) is null and 
                    src:dlcn_ref_compnr is not null) THEN
                    'dlcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dlcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dler_ref_compnr), '0'), 38, 10) is null and 
                    src:dler_ref_compnr is not null) THEN
                    'dler_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dler_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:eftm), '1900-01-01')) is null and 
                    src:eftm is not null) THEN
                    'eftm contains a non-timestamp value : \'' || AS_VARCHAR(src:eftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcad_ref_compnr), '0'), 38, 10) is null and 
                    src:lcad_ref_compnr is not null) THEN
                    'lcad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lste_ref_compnr), '0'), 38, 10) is null and 
                    src:lste_ref_compnr is not null) THEN
                    'lste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:mdpt_ref_compnr is not null) THEN
                    'mdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpt_ref_compnr), '0'), 38, 10) is null and 
                    src:odpt_ref_compnr is not null) THEN
                    'odpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:odpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) THEN
                    'ortp contains a non-numeric value : \'' || AS_VARCHAR(src:ortp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owtp), '0'), 38, 10) is null and 
                    src:owtp is not null) THEN
                    'owtp contains a non-numeric value : \'' || AS_VARCHAR(src:owtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prfa_ref_compnr), '0'), 38, 10) is null and 
                    src:prfa_ref_compnr is not null) THEN
                    'prfa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prfa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prfb_ref_compnr), '0'), 38, 10) is null and 
                    src:prfb_ref_compnr is not null) THEN
                    'prfb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prfb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) THEN
                    'prio contains a non-numeric value : \'' || AS_VARCHAR(src:prio) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is null and 
                    src:prio_ref_compnr is not null) THEN
                    'prio_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prio_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rspe_ref_compnr), '0'), 38, 10) is null and 
                    src:rspe_ref_compnr is not null) THEN
                    'rspe_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rspe_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is null and 
                    src:rste_ref_compnr is not null) THEN
                    'rste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdno), '0'), 38, 10) is null and 
                    src:sdno is not null) THEN
                    'sdno contains a non-numeric value : \'' || AS_VARCHAR(src:sdno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soff_ref_compnr), '0'), 38, 10) is null and 
                    src:soff_ref_compnr is not null) THEN
                    'soff_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:soff_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spno), '0'), 38, 10) is null and 
                    src:spno is not null) THEN
                    'spno contains a non-numeric value : \'' || AS_VARCHAR(src:spno) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdt), '0'), 38, 10) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-numeric value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trtm), '0'), 38, 10) is null and 
                    src:trtm is not null) THEN
                    'trtm contains a non-numeric value : \'' || AS_VARCHAR(src:trtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is null and 
                    src:ubad_ref_compnr is not null) THEN
                    'ubad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ubbp_ref_compnr is not null) THEN
                    'ubbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ubcn_ref_compnr is not null) THEN
                    'ubcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is null and 
                    src:ubop_ref_compnr is not null) THEN
                    'ubop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ubop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubpc), '0'), 38, 10) is null and 
                    src:ubpc is not null) THEN
                    'ubpc contains a non-numeric value : \'' || AS_VARCHAR(src:ubpc) || '\'' WHEN 
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
                src:clst  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSBSC100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfad_ref_compnr), '0'), 38, 10) is null and 
                    src:bfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:bfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccha), '0'), 38, 10) is null and 
                    src:ccha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cusc_ref_compnr), '0'), 38, 10) is null and 
                    src:cusc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:czon_ref_compnr), '0'), 38, 10) is null and 
                    src:czon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbpo), '0'), 38, 10) is null and 
                    src:dbpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlad_ref_compnr), '0'), 38, 10) is null and 
                    src:dlad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlcn_ref_compnr), '0'), 38, 10) is null and 
                    src:dlcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dler_ref_compnr), '0'), 38, 10) is null and 
                    src:dler_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:eftm), '1900-01-01')) is null and 
                    src:eftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcad_ref_compnr), '0'), 38, 10) is null and 
                    src:lcad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lste_ref_compnr), '0'), 38, 10) is null and 
                    src:lste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:mdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odpt_ref_compnr), '0'), 38, 10) is null and 
                    src:odpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owtp), '0'), 38, 10) is null and 
                    src:owtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prfa_ref_compnr), '0'), 38, 10) is null and 
                    src:prfa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prfb_ref_compnr), '0'), 38, 10) is null and 
                    src:prfb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is null and 
                    src:prio_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rspe_ref_compnr), '0'), 38, 10) is null and 
                    src:rspe_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is null and 
                    src:rste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdno), '0'), 38, 10) is null and 
                    src:sdno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soff_ref_compnr), '0'), 38, 10) is null and 
                    src:soff_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spno), '0'), 38, 10) is null and 
                    src:spno is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdt), '0'), 38, 10) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trtm), '0'), 38, 10) is null and 
                    src:trtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is null and 
                    src:ubad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ubbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ubcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is null and 
                    src:ubop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubpc), '0'), 38, 10) is null and 
                    src:ubpc is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)