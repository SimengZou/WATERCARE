CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPIN060_ERROR AS SELECT src, 'LN_TPPIN060' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:addt), '1900-01-01')) is null and 
                    src:addt is not null) THEN
                    'addt contains a non-timestamp value : \'' || AS_VARCHAR(src:addt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) THEN
                    'carr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) THEN
                    'cono_cnln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) THEN
                    'cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlld), '1900-01-01')) is null and 
                    src:dlld is not null) THEN
                    'dlld contains a non-timestamp value : \'' || AS_VARCHAR(src:dlld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlor), '0'), 38, 10) is null and 
                    src:dlor is not null) THEN
                    'dlor contains a non-numeric value : \'' || AS_VARCHAR(src:dlor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) THEN
                    'dlvr contains a non-numeric value : \'' || AS_VARCHAR(src:dlvr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg), '0'), 38, 10) is null and 
                    src:dorg is not null) THEN
                    'dorg contains a non-numeric value : \'' || AS_VARCHAR(src:dorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icmp), '0'), 38, 10) is null and 
                    src:icmp is not null) THEN
                    'icmp contains a non-numeric value : \'' || AS_VARCHAR(src:icmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idln), '0'), 38, 10) is null and 
                    src:idln is not null) THEN
                    'idln contains a non-numeric value : \'' || AS_VARCHAR(src:idln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam), '0'), 38, 10) is null and 
                    src:ldam is not null) THEN
                    'ldam contains a non-numeric value : \'' || AS_VARCHAR(src:ldam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:link), '0'), 38, 10) is null and 
                    src:link is not null) THEN
                    'link contains a non-numeric value : \'' || AS_VARCHAR(src:link) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is null and 
                    src:lssn_ref_compnr is not null) THEN
                    'lssn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lssn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) THEN
                    'nins contains a non-numeric value : \'' || AS_VARCHAR(src:nins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcad), '0'), 38, 10) is null and 
                    src:pcad is not null) THEN
                    'pcad contains a non-numeric value : \'' || AS_VARCHAR(src:pcad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) THEN
                    'porg contains a non-numeric value : \'' || AS_VARCHAR(src:porg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pupd), '0'), 38, 10) is null and 
                    src:pupd is not null) THEN
                    'pupd contains a non-numeric value : \'' || AS_VARCHAR(src:pupd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qshp), '0'), 38, 10) is null and 
                    src:qshp is not null) THEN
                    'qshp contains a non-numeric value : \'' || AS_VARCHAR(src:qshp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) THEN
                    'rcln contains a non-numeric value : \'' || AS_VARCHAR(src:rcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtor), '0'), 38, 10) is null and 
                    src:rtor is not null) THEN
                    'rtor contains a non-numeric value : \'' || AS_VARCHAR(src:rtor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt), '0'), 38, 10) is null and 
                    src:samt is not null) THEN
                    'samt contains a non-numeric value : \'' || AS_VARCHAR(src:samt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_cntc), '0'), 38, 10) is null and 
                    src:samt_cntc is not null) THEN
                    'samt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_dwhc), '0'), 38, 10) is null and 
                    src:samt_dwhc is not null) THEN
                    'samt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_homc), '0'), 38, 10) is null and 
                    src:samt_homc is not null) THEN
                    'samt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_prjc), '0'), 38, 10) is null and 
                    src:samt_prjc is not null) THEN
                    'samt_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_refc), '0'), 38, 10) is null and 
                    src:samt_refc is not null) THEN
                    'samt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:samt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc1), '0'), 38, 10) is null and 
                    src:samt_rpc1 is not null) THEN
                    'samt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:samt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc2), '0'), 38, 10) is null and 
                    src:samt_rpc2 is not null) THEN
                    'samt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:samt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) THEN
                    'schd contains a non-numeric value : \'' || AS_VARCHAR(src:schd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srvc), '0'), 38, 10) is null and 
                    src:srvc is not null) THEN
                    'srvc contains a non-numeric value : \'' || AS_VARCHAR(src:srvc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is null and 
                    src:twar_ref_compnr is not null) THEN
                    'twar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:twar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
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
                                    
                src:shpm,
                src:sern,
                src:cprj,
                src:compnr,
                src:rcno,
                src:schd,
                src:pono,
                src:cono,
                src:cact,
                src:cpla,
                src:rcln,
                src:dlvr,
                src:cspa,
                src:cnln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPIN060 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:addt), '1900-01-01')) is null and 
                    src:addt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlld), '1900-01-01')) is null and 
                    src:dlld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlor), '0'), 38, 10) is null and 
                    src:dlor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg), '0'), 38, 10) is null and 
                    src:dorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icmp), '0'), 38, 10) is null and 
                    src:icmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idln), '0'), 38, 10) is null and 
                    src:idln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam), '0'), 38, 10) is null and 
                    src:ldam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:link), '0'), 38, 10) is null and 
                    src:link is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is null and 
                    src:lssn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcad), '0'), 38, 10) is null and 
                    src:pcad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pupd), '0'), 38, 10) is null and 
                    src:pupd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qshp), '0'), 38, 10) is null and 
                    src:qshp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtor), '0'), 38, 10) is null and 
                    src:rtor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt), '0'), 38, 10) is null and 
                    src:samt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_cntc), '0'), 38, 10) is null and 
                    src:samt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_dwhc), '0'), 38, 10) is null and 
                    src:samt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_homc), '0'), 38, 10) is null and 
                    src:samt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_prjc), '0'), 38, 10) is null and 
                    src:samt_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_refc), '0'), 38, 10) is null and 
                    src:samt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc1), '0'), 38, 10) is null and 
                    src:samt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:samt_rpc2), '0'), 38, 10) is null and 
                    src:samt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srvc), '0'), 38, 10) is null and 
                    src:srvc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is null and 
                    src:twar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)