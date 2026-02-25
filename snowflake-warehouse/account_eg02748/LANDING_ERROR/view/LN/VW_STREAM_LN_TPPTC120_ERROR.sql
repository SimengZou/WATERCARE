CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC120_ERROR AS SELECT src, 'LN_TPPTC120' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) THEN
                    'amoc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) THEN
                    'amos contains a non-numeric value : \'' || AS_VARCHAR(src:amos) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) THEN
                    'btdt contains a non-timestamp value : \'' || AS_VARCHAR(src:btdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is null and 
                    src:clas_ref_compnr is not null) THEN
                    'clas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) THEN
                    'cocu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cocu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona), '0'), 38, 10) is null and 
                    src:cona is not null) THEN
                    'cona contains a non-numeric value : \'' || AS_VARCHAR(src:cona) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) THEN
                    'cpva contains a non-numeric value : \'' || AS_VARCHAR(src:cpva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbgf), '0'), 38, 10) is null and 
                    src:dbgf is not null) THEN
                    'dbgf contains a non-numeric value : \'' || AS_VARCHAR(src:dbgf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpc), '0'), 38, 10) is null and 
                    src:dfpc is not null) THEN
                    'dfpc contains a non-numeric value : \'' || AS_VARCHAR(src:dfpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) THEN
                    'dlvr contains a non-numeric value : \'' || AS_VARCHAR(src:dlvr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcta), '0'), 38, 10) is null and 
                    src:lcta is not null) THEN
                    'lcta contains a non-numeric value : \'' || AS_VARCHAR(src:lcta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) THEN
                    'leng contains a non-numeric value : \'' || AS_VARCHAR(src:leng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noun), '0'), 38, 10) is null and 
                    src:noun is not null) THEN
                    'noun contains a non-numeric value : \'' || AS_VARCHAR(src:noun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) THEN
                    'rtcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) THEN
                    'rtcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) THEN
                    'rtcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is null and 
                    src:rtcs_1 is not null) THEN
                    'rtcs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is null and 
                    src:rtcs_2 is not null) THEN
                    'rtcs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is null and 
                    src:rtcs_3 is not null) THEN
                    'rtcs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtcs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) THEN
                    'rtfc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) THEN
                    'rtfc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) THEN
                    'rtfc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is null and 
                    src:rtfs_1 is not null) THEN
                    'rtfs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is null and 
                    src:rtfs_2 is not null) THEN
                    'rtfs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is null and 
                    src:rtfs_3 is not null) THEN
                    'rtfs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtfs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) THEN
                    'sacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) THEN
                    'schd contains a non-numeric value : \'' || AS_VARCHAR(src:schd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) THEN
                    'scpf contains a non-numeric value : \'' || AS_VARCHAR(src:scpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trts), '0'), 38, 10) is null and 
                    src:trts is not null) THEN
                    'trts contains a non-numeric value : \'' || AS_VARCHAR(src:trts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsrl), '0'), 38, 10) is null and 
                    src:tsrl is not null) THEN
                    'tsrl contains a non-numeric value : \'' || AS_VARCHAR(src:tsrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) THEN
                    'widt contains a non-numeric value : \'' || AS_VARCHAR(src:widt) || '\'' WHEN 
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
                                    
                src:cprj,
                src:compnr,
                src:cspa,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC120 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is null and 
                    src:clas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona), '0'), 38, 10) is null and 
                    src:cona is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbgf), '0'), 38, 10) is null and 
                    src:dbgf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpc), '0'), 38, 10) is null and 
                    src:dfpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlvr), '0'), 38, 10) is null and 
                    src:dlvr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcta), '0'), 38, 10) is null and 
                    src:lcta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leng), '0'), 38, 10) is null and 
                    src:leng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noun), '0'), 38, 10) is null and 
                    src:noun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is null and 
                    src:rtcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is null and 
                    src:rtcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is null and 
                    src:rtcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is null and 
                    src:rtcs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is null and 
                    src:rtcs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is null and 
                    src:rtcs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is null and 
                    src:rtfc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is null and 
                    src:rtfc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is null and 
                    src:rtfc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is null and 
                    src:rtfs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is null and 
                    src:rtfs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is null and 
                    src:rtfs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schd), '0'), 38, 10) is null and 
                    src:schd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trts), '0'), 38, 10) is null and 
                    src:trts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsrl), '0'), 38, 10) is null and 
                    src:tsrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:widt), '0'), 38, 10) is null and 
                    src:widt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)