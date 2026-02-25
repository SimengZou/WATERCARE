CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPIN040_ERROR AS SELECT src, 'LN_TPPIN040' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clos), '0'), 38, 10) is null and 
                    src:clos is not null) THEN
                    'clos contains a non-numeric value : \'' || AS_VARCHAR(src:clos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cncl), '0'), 38, 10) is null and 
                    src:cncl is not null) THEN
                    'cncl contains a non-numeric value : \'' || AS_VARCHAR(src:cncl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_tact_ref_compnr is not null) THEN
                    'cprj_cpla_tact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_tact_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) THEN
                    'dlvr contains a non-numeric value : \'' || AS_VARCHAR(src:dlvr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) THEN
                    'fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gseq), '0'), 38, 10) is null and 
                    src:gseq is not null) THEN
                    'gseq contains a non-numeric value : \'' || AS_VARCHAR(src:gseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai), '0'), 38, 10) is null and 
                    src:hbai is not null) THEN
                    'hbai contains a non-numeric value : \'' || AS_VARCHAR(src:hbai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_cntc), '0'), 38, 10) is null and 
                    src:hbai_cntc is not null) THEN
                    'hbai_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_dwhc), '0'), 38, 10) is null and 
                    src:hbai_dwhc is not null) THEN
                    'hbai_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_homc), '0'), 38, 10) is null and 
                    src:hbai_homc is not null) THEN
                    'hbai_homc contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_prjc), '0'), 38, 10) is null and 
                    src:hbai_prjc is not null) THEN
                    'hbai_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_refc), '0'), 38, 10) is null and 
                    src:hbai_refc is not null) THEN
                    'hbai_refc contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_rpc1), '0'), 38, 10) is null and 
                    src:hbai_rpc1 is not null) THEN
                    'hbai_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_rpc2), '0'), 38, 10) is null and 
                    src:hbai_rpc2 is not null) THEN
                    'hbai_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:hbai_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) THEN
                    'hbnr contains a non-numeric value : \'' || AS_VARCHAR(src:hbnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) THEN
                    'hbpc contains a non-numeric value : \'' || AS_VARCHAR(src:hbpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai), '0'), 38, 10) is null and 
                    src:hiai is not null) THEN
                    'hiai contains a non-numeric value : \'' || AS_VARCHAR(src:hiai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_cntc), '0'), 38, 10) is null and 
                    src:hiai_cntc is not null) THEN
                    'hiai_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_dwhc), '0'), 38, 10) is null and 
                    src:hiai_dwhc is not null) THEN
                    'hiai_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_homc), '0'), 38, 10) is null and 
                    src:hiai_homc is not null) THEN
                    'hiai_homc contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_prjc), '0'), 38, 10) is null and 
                    src:hiai_prjc is not null) THEN
                    'hiai_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_refc), '0'), 38, 10) is null and 
                    src:hiai_refc is not null) THEN
                    'hiai_refc contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_rpc1), '0'), 38, 10) is null and 
                    src:hiai_rpc1 is not null) THEN
                    'hiai_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_rpc2), '0'), 38, 10) is null and 
                    src:hiai_rpc2 is not null) THEN
                    'hiai_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:hiai_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai), '0'), 38, 10) is null and 
                    src:htai is not null) THEN
                    'htai contains a non-numeric value : \'' || AS_VARCHAR(src:htai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_cntc), '0'), 38, 10) is null and 
                    src:htai_cntc is not null) THEN
                    'htai_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:htai_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_dwhc), '0'), 38, 10) is null and 
                    src:htai_dwhc is not null) THEN
                    'htai_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:htai_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_homc), '0'), 38, 10) is null and 
                    src:htai_homc is not null) THEN
                    'htai_homc contains a non-numeric value : \'' || AS_VARCHAR(src:htai_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_prjc), '0'), 38, 10) is null and 
                    src:htai_prjc is not null) THEN
                    'htai_prjc contains a non-numeric value : \'' || AS_VARCHAR(src:htai_prjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_refc), '0'), 38, 10) is null and 
                    src:htai_refc is not null) THEN
                    'htai_refc contains a non-numeric value : \'' || AS_VARCHAR(src:htai_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_rpc1), '0'), 38, 10) is null and 
                    src:htai_rpc1 is not null) THEN
                    'htai_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:htai_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_rpc2), '0'), 38, 10) is null and 
                    src:htai_rpc2 is not null) THEN
                    'htai_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:htai_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) THEN
                    'invo contains a non-numeric value : \'' || AS_VARCHAR(src:invo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) THEN
                    'nins contains a non-numeric value : \'' || AS_VARCHAR(src:nins) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrn), '0'), 38, 10) is null and 
                    src:osrn is not null) THEN
                    'osrn contains a non-numeric value : \'' || AS_VARCHAR(src:osrn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is null and 
                    src:ovhd_ref_compnr is not null) THEN
                    'ovhd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ovhd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) THEN
                    'pcwa contains a non-numeric value : \'' || AS_VARCHAR(src:pcwa) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) THEN
                    'schd contains a non-numeric value : \'' || AS_VARCHAR(src:schd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:urdt), '1900-01-01')) is null and 
                    src:urdt is not null) THEN
                    'urdt contains a non-timestamp value : \'' || AS_VARCHAR(src:urdt) || '\'' WHEN 
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
                src:cnln,
                src:cono,
                src:ofbp,
                src:hbnr,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPIN040 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clos), '0'), 38, 10) is null and 
                    src:clos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cncl), '0'), 38, 10) is null and 
                    src:cncl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_tact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_tact_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcmp), '0'), 38, 10) is null and 
                    src:fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gseq), '0'), 38, 10) is null and 
                    src:gseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai), '0'), 38, 10) is null and 
                    src:hbai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_cntc), '0'), 38, 10) is null and 
                    src:hbai_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_dwhc), '0'), 38, 10) is null and 
                    src:hbai_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_homc), '0'), 38, 10) is null and 
                    src:hbai_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_prjc), '0'), 38, 10) is null and 
                    src:hbai_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_refc), '0'), 38, 10) is null and 
                    src:hbai_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_rpc1), '0'), 38, 10) is null and 
                    src:hbai_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbai_rpc2), '0'), 38, 10) is null and 
                    src:hbai_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbnr), '0'), 38, 10) is null and 
                    src:hbnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai), '0'), 38, 10) is null and 
                    src:hiai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_cntc), '0'), 38, 10) is null and 
                    src:hiai_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_dwhc), '0'), 38, 10) is null and 
                    src:hiai_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_homc), '0'), 38, 10) is null and 
                    src:hiai_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_prjc), '0'), 38, 10) is null and 
                    src:hiai_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_refc), '0'), 38, 10) is null and 
                    src:hiai_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_rpc1), '0'), 38, 10) is null and 
                    src:hiai_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiai_rpc2), '0'), 38, 10) is null and 
                    src:hiai_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai), '0'), 38, 10) is null and 
                    src:htai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_cntc), '0'), 38, 10) is null and 
                    src:htai_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_dwhc), '0'), 38, 10) is null and 
                    src:htai_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_homc), '0'), 38, 10) is null and 
                    src:htai_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_prjc), '0'), 38, 10) is null and 
                    src:htai_prjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_refc), '0'), 38, 10) is null and 
                    src:htai_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_rpc1), '0'), 38, 10) is null and 
                    src:htai_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:htai_rpc2), '0'), 38, 10) is null and 
                    src:htai_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invo), '0'), 38, 10) is null and 
                    src:invo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nins), '0'), 38, 10) is null and 
                    src:nins is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrn), '0'), 38, 10) is null and 
                    src:osrn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovhd_ref_compnr), '0'), 38, 10) is null and 
                    src:ovhd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) or 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:urdt), '1900-01-01')) is null and 
                    src:urdt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)