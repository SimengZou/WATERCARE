CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM300_ERROR AS SELECT src, 'LN_TSCTM300' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) THEN
                    'bppr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cadt), '1900-01-01')) is null and 
                    src:cadt is not null) THEN
                    'cadt contains a non-timestamp value : \'' || AS_VARCHAR(src:cadt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccqu_ref_compnr), '0'), 38, 10) is null and 
                    src:ccqu_ref_compnr is not null) THEN
                    'ccqu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccqu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) THEN
                    'ccty_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) THEN
                    'cdis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cind_ref_compnr), '0'), 38, 10) is null and 
                    src:cind_ref_compnr is not null) THEN
                    'cind_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cind_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplt_ref_compnr), '0'), 38, 10) is null and 
                    src:cplt_ref_compnr is not null) THEN
                    'cplt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cplt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crnp), '0'), 38, 10) is null and 
                    src:crnp is not null) THEN
                    'crnp contains a non-numeric value : \'' || AS_VARCHAR(src:crnp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpu), '0'), 38, 10) is null and 
                    src:crpu is not null) THEN
                    'crpu contains a non-numeric value : \'' || AS_VARCHAR(src:crpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cryn), '0'), 38, 10) is null and 
                    src:cryn is not null) THEN
                    'cryn contains a non-numeric value : \'' || AS_VARCHAR(src:cryn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctin_ref_compnr), '0'), 38, 10) is null and 
                    src:ctin_ref_compnr is not null) THEN
                    'ctin_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctin_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctpc_ref_compnr), '0'), 38, 10) is null and 
                    src:ctpc_ref_compnr is not null) THEN
                    'ctpc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctpc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpb), '0'), 38, 10) is null and 
                    src:dfpb is not null) THEN
                    'dfpb contains a non-numeric value : \'' || AS_VARCHAR(src:dfpb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ditc), '0'), 38, 10) is null and 
                    src:ditc is not null) THEN
                    'ditc contains a non-numeric value : \'' || AS_VARCHAR(src:ditc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) THEN
                    'exmt contains a non-numeric value : \'' || AS_VARCHAR(src:exmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) THEN
                    'exrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpn), '0'), 38, 10) is null and 
                    src:icpn is not null) THEN
                    'icpn contains a non-numeric value : \'' || AS_VARCHAR(src:icpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inch), '0'), 38, 10) is null and 
                    src:inch is not null) THEN
                    'inch contains a non-numeric value : \'' || AS_VARCHAR(src:inch) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:indx), '0'), 38, 10) is null and 
                    src:indx is not null) THEN
                    'indx contains a non-numeric value : \'' || AS_VARCHAR(src:indx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:innp), '0'), 38, 10) is null and 
                    src:innp is not null) THEN
                    'innp contains a non-numeric value : \'' || AS_VARCHAR(src:innp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpc), '0'), 38, 10) is null and 
                    src:inpc is not null) THEN
                    'inpc contains a non-numeric value : \'' || AS_VARCHAR(src:inpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpu), '0'), 38, 10) is null and 
                    src:inpu is not null) THEN
                    'inpu contains a non-numeric value : \'' || AS_VARCHAR(src:inpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) THEN
                    'itad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) THEN
                    'itcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mexp), '0'), 38, 10) is null and 
                    src:mexp is not null) THEN
                    'mexp contains a non-numeric value : \'' || AS_VARCHAR(src:mexp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpe), '0'), 38, 10) is null and 
                    src:nrpe is not null) THEN
                    'nrpe contains a non-numeric value : \'' || AS_VARCHAR(src:nrpe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orsc_ref_compnr), '0'), 38, 10) is null and 
                    src:orsc_ref_compnr is not null) THEN
                    'orsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orsc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovyn), '0'), 38, 10) is null and 
                    src:ovyn is not null) THEN
                    'ovyn contains a non-numeric value : \'' || AS_VARCHAR(src:ovyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) THEN
                    'perc contains a non-numeric value : \'' || AS_VARCHAR(src:perc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peru), '0'), 38, 10) is null and 
                    src:peru is not null) THEN
                    'peru contains a non-numeric value : \'' || AS_VARCHAR(src:peru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) THEN
                    'pfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmt), '0'), 38, 10) is null and 
                    src:prmt is not null) THEN
                    'prmt contains a non-numeric value : \'' || AS_VARCHAR(src:prmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prov), '0'), 38, 10) is null and 
                    src:prov is not null) THEN
                    'prov contains a non-numeric value : \'' || AS_VARCHAR(src:prov) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) THEN
                    'ratc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) THEN
                    'ratc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) THEN
                    'ratc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rccg), '0'), 38, 10) is null and 
                    src:rccg is not null) THEN
                    'rccg contains a non-numeric value : \'' || AS_VARCHAR(src:rccg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) THEN
                    'rtdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rtdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvmd), '0'), 38, 10) is null and 
                    src:rvmd is not null) THEN
                    'rvmd contains a non-numeric value : \'' || AS_VARCHAR(src:rvmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc), '0'), 38, 10) is null and 
                    src:txtc is not null) THEN
                    'txtc contains a non-numeric value : \'' || AS_VARCHAR(src:txtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is null and 
                    src:txtc_ref_compnr is not null) THEN
                    'txtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd), '0'), 38, 10) is null and 
                    src:txtd is not null) THEN
                    'txtd contains a non-numeric value : \'' || AS_VARCHAR(src:txtd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd_ref_compnr), '0'), 38, 10) is null and 
                    src:txtd_ref_compnr is not null) THEN
                    'txtd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte), '0'), 38, 10) is null and 
                    src:txte is not null) THEN
                    'txte contains a non-numeric value : \'' || AS_VARCHAR(src:txte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte_ref_compnr), '0'), 38, 10) is null and 
                    src:txte_ref_compnr is not null) THEN
                    'txte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txyn), '0'), 38, 10) is null and 
                    src:txyn is not null) THEN
                    'txyn contains a non-numeric value : \'' || AS_VARCHAR(src:txyn) || '\'' WHEN 
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
                                    
                src:csco,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM300 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cadt), '1900-01-01')) is null and 
                    src:cadt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccqu_ref_compnr), '0'), 38, 10) is null and 
                    src:ccqu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cind_ref_compnr), '0'), 38, 10) is null and 
                    src:cind_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplt_ref_compnr), '0'), 38, 10) is null and 
                    src:cplt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crnp), '0'), 38, 10) is null and 
                    src:crnp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpu), '0'), 38, 10) is null and 
                    src:crpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cryn), '0'), 38, 10) is null and 
                    src:cryn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctin_ref_compnr), '0'), 38, 10) is null and 
                    src:ctin_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctpc_ref_compnr), '0'), 38, 10) is null and 
                    src:ctpc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpb), '0'), 38, 10) is null and 
                    src:dfpb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ditc), '0'), 38, 10) is null and 
                    src:ditc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exmt), '0'), 38, 10) is null and 
                    src:exmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is null and 
                    src:exrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpn), '0'), 38, 10) is null and 
                    src:icpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inch), '0'), 38, 10) is null and 
                    src:inch is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:indx), '0'), 38, 10) is null and 
                    src:indx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:innp), '0'), 38, 10) is null and 
                    src:innp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpc), '0'), 38, 10) is null and 
                    src:inpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpu), '0'), 38, 10) is null and 
                    src:inpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is null and 
                    src:itcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mexp), '0'), 38, 10) is null and 
                    src:mexp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpe), '0'), 38, 10) is null and 
                    src:nrpe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orsc_ref_compnr), '0'), 38, 10) is null and 
                    src:orsc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovyn), '0'), 38, 10) is null and 
                    src:ovyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peru), '0'), 38, 10) is null and 
                    src:peru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:pfcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmt), '0'), 38, 10) is null and 
                    src:prmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prov), '0'), 38, 10) is null and 
                    src:prov is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rccg), '0'), 38, 10) is null and 
                    src:rccg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvmd), '0'), 38, 10) is null and 
                    src:rvmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc), '0'), 38, 10) is null and 
                    src:txtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is null and 
                    src:txtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd), '0'), 38, 10) is null and 
                    src:txtd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtd_ref_compnr), '0'), 38, 10) is null and 
                    src:txtd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte), '0'), 38, 10) is null and 
                    src:txte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txte_ref_compnr), '0'), 38, 10) is null and 
                    src:txte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txyn), '0'), 38, 10) is null and 
                    src:txyn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)