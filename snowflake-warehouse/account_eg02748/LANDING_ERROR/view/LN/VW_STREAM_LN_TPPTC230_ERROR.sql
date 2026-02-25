CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC230_ERROR AS SELECT src, 'LN_TPPTC230' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) THEN
                    'acln contains a non-numeric value : \'' || AS_VARCHAR(src:acln) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) THEN
                    'cocu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cocu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona), '0'), 38, 10) is null and 
                    src:cona is not null) THEN
                    'cona contains a non-numeric value : \'' || AS_VARCHAR(src:cona) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copy), '0'), 38, 10) is null and 
                    src:copy is not null) THEN
                    'copy contains a non-numeric value : \'' || AS_VARCHAR(src:copy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_sero_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_sero_ref_compnr is not null) THEN
                    'cprj_cpla_cact_sero_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_sero_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrg_ref_compnr is not null) THEN
                    'ctrg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctrg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrc), '0'), 38, 10) is null and 
                    src:dfrc is not null) THEN
                    'dfrc contains a non-numeric value : \'' || AS_VARCHAR(src:dfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) THEN
                    'dura contains a non-numeric value : \'' || AS_VARCHAR(src:dura) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eplf), '0'), 38, 10) is null and 
                    src:eplf is not null) THEN
                    'eplf contains a non-numeric value : \'' || AS_VARCHAR(src:eplf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) THEN
                    'freq contains a non-numeric value : \'' || AS_VARCHAR(src:freq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) THEN
                    'lino contains a non-numeric value : \'' || AS_VARCHAR(src:lino) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norm), '0'), 38, 10) is null and 
                    src:norm is not null) THEN
                    'norm contains a non-numeric value : \'' || AS_VARCHAR(src:norm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsr), '0'), 38, 10) is null and 
                    src:prsr is not null) THEN
                    'prsr contains a non-numeric value : \'' || AS_VARCHAR(src:prsr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) THEN
                    'ratc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) THEN
                    'rats contains a non-numeric value : \'' || AS_VARCHAR(src:rats) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rfin), '1900-01-01')) is null and 
                    src:rfin is not null) THEN
                    'rfin contains a non-timestamp value : \'' || AS_VARCHAR(src:rfin) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rsta), '1900-01-01')) is null and 
                    src:rsta is not null) THEN
                    'rsta contains a non-timestamp value : \'' || AS_VARCHAR(src:rsta) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sero), '0'), 38, 10) is null and 
                    src:sero is not null) THEN
                    'sero contains a non-numeric value : \'' || AS_VARCHAR(src:sero) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slnk), '0'), 38, 10) is null and 
                    src:slnk is not null) THEN
                    'slnk contains a non-numeric value : \'' || AS_VARCHAR(src:slnk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:snsb), '0'), 38, 10) is null and 
                    src:snsb is not null) THEN
                    'snsb contains a non-numeric value : \'' || AS_VARCHAR(src:snsb) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unrt_ref_compnr), '0'), 38, 10) is null and 
                    src:unrt_ref_compnr is not null) THEN
                    'unrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unrt_ref_compnr) || '\'' WHEN 
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
                                    
                src:cact,
                src:compnr,
                src:cprj,
                src:cpla,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC230 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is null and 
                    src:cocu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cona), '0'), 38, 10) is null and 
                    src:cona is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copy), '0'), 38, 10) is null and 
                    src:copy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_sero_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_sero_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrc), '0'), 38, 10) is null and 
                    src:dfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eplf), '0'), 38, 10) is null and 
                    src:eplf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norm), '0'), 38, 10) is null and 
                    src:norm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsr), '0'), 38, 10) is null and 
                    src:prsr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rfin), '1900-01-01')) is null and 
                    src:rfin is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rsta), '1900-01-01')) is null and 
                    src:rsta is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sero), '0'), 38, 10) is null and 
                    src:sero is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slnk), '0'), 38, 10) is null and 
                    src:slnk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:snsb), '0'), 38, 10) is null and 
                    src:snsb is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unrt_ref_compnr), '0'), 38, 10) is null and 
                    src:unrt_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)