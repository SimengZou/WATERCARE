CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPIN010_ERROR AS SELECT src, 'LN_TPPIN010' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adsu), '0'), 38, 10) is null and 
                    src:adsu is not null) THEN
                    'adsu contains a non-numeric value : \'' || AS_VARCHAR(src:adsu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_cntc), '0'), 38, 10) is null and 
                    src:amnt_cntc is not null) THEN
                    'amnt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is null and 
                    src:amnt_dwhc is not null) THEN
                    'amnt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is null and 
                    src:amnt_homc is not null) THEN
                    'amnt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_prjc), '0'), 38, 10) is null and 
                    src:amnt_prjc is not null) THEN
                    'amnt_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is null and 
                    src:amnt_refc is not null) THEN
                    'amnt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) THEN
                    'amnt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) THEN
                    'amnt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_cact_ref_compnr is not null) THEN
                    'cnpr_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_ref_compnr is not null) THEN
                    'cnpr_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_tact_ref_compnr is not null) THEN
                    'cnpr_cpla_tact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cspa_ref_compnr is not null) THEN
                    'cnpr_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_ref_compnr is not null) THEN
                    'cnpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cnpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_nins_cprj_ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_nins_cprj_ofbp_ref_compnr is not null) THEN
                    'cono_cnln_nins_cprj_ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_nins_cprj_ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_oadv_cprj_ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_oadv_cprj_ofbp_ref_compnr is not null) THEN
                    'cono_cnln_oadv_cprj_ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_oadv_cprj_ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) THEN
                    'cono_cnln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_cnln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dnse), '0'), 38, 10) is null and 
                    src:dnse is not null) THEN
                    'dnse contains a non-numeric value : \'' || AS_VARCHAR(src:dnse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) THEN
                    'invo contains a non-numeric value : \'' || AS_VARCHAR(src:invo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) THEN
                    'nins contains a non-numeric value : \'' || AS_VARCHAR(src:nins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oadv), '0'), 38, 10) is null and 
                    src:oadv is not null) THEN
                    'oadv contains a non-numeric value : \'' || AS_VARCHAR(src:oadv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdbo), '0'), 38, 10) is null and 
                    src:pdbo is not null) THEN
                    'pdbo contains a non-numeric value : \'' || AS_VARCHAR(src:pdbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) THEN
                    'rate_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) THEN
                    'rate_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) THEN
                    'rate_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) THEN
                    'rdas contains a non-timestamp value : \'' || AS_VARCHAR(src:rdas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sidt), '1900-01-01')) is null and 
                    src:sidt is not null) THEN
                    'sidt contains a non-timestamp value : \'' || AS_VARCHAR(src:sidt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toin), '0'), 38, 10) is null and 
                    src:toin is not null) THEN
                    'toin contains a non-numeric value : \'' || AS_VARCHAR(src:toin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) THEN
                    'trsl contains a non-numeric value : \'' || AS_VARCHAR(src:trsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                src:ofbp,
                src:compnr,
                src:cono,
                src:cnln,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPIN010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adsu), '0'), 38, 10) is null and 
                    src:adsu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_cntc), '0'), 38, 10) is null and 
                    src:amnt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is null and 
                    src:amnt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is null and 
                    src:amnt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_prjc), '0'), 38, 10) is null and 
                    src:amnt_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is null and 
                    src:amnt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cpla_tact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is null and 
                    src:cnpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_nins_cprj_ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_nins_cprj_ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_oadv_cprj_ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_oadv_cprj_ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_cnln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dnse), '0'), 38, 10) is null and 
                    src:dnse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oadv), '0'), 38, 10) is null and 
                    src:oadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdbo), '0'), 38, 10) is null and 
                    src:pdbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sidt), '1900-01-01')) is null and 
                    src:sidt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toin), '0'), 38, 10) is null and 
                    src:toin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trsl), '0'), 38, 10) is null and 
                    src:trsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)