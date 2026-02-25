CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM790_ERROR AS SELECT src, 'LN_TPPDM790' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmp), '0'), 38, 10) is null and 
                    src:acmp is not null) THEN
                    'acmp contains a non-numeric value : \'' || AS_VARCHAR(src:acmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agty), '0'), 38, 10) is null and 
                    src:agty is not null) THEN
                    'agty contains a non-numeric value : \'' || AS_VARCHAR(src:agty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arch), '0'), 38, 10) is null and 
                    src:arch is not null) THEN
                    'arch contains a non-numeric value : \'' || AS_VARCHAR(src:arch) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ardt), '1900-01-01')) is null and 
                    src:ardt is not null) THEN
                    'ardt contains a non-timestamp value : \'' || AS_VARCHAR(src:ardt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) THEN
                    'capp contains a non-numeric value : \'' || AS_VARCHAR(src:capp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) THEN
                    'ccam_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccam_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) THEN
                    'ccot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) THEN
                    'cfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmud_ref_compnr), '0'), 38, 10) is null and 
                    src:cmud_ref_compnr is not null) THEN
                    'cmud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cobs_ref_compnr), '0'), 38, 10) is null and 
                    src:cobs_ref_compnr is not null) THEN
                    'cobs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cobs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) THEN
                    'coty contains a non-numeric value : \'' || AS_VARCHAR(src:coty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) THEN
                    'dldt contains a non-timestamp value : \'' || AS_VARCHAR(src:dldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espt), '0'), 38, 10) is null and 
                    src:espt is not null) THEN
                    'espt contains a non-numeric value : \'' || AS_VARCHAR(src:espt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fddt), '1900-01-01')) is null and 
                    src:fddt is not null) THEN
                    'fddt contains a non-timestamp value : \'' || AS_VARCHAR(src:fddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intp), '0'), 38, 10) is null and 
                    src:intp is not null) THEN
                    'intp contains a non-numeric value : \'' || AS_VARCHAR(src:intp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) THEN
                    'invm contains a non-numeric value : \'' || AS_VARCHAR(src:invm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipin), '0'), 38, 10) is null and 
                    src:ipin is not null) THEN
                    'ipin contains a non-numeric value : \'' || AS_VARCHAR(src:ipin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kopr), '0'), 38, 10) is null and 
                    src:kopr is not null) THEN
                    'kopr contains a non-numeric value : \'' || AS_VARCHAR(src:kopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kptm), '0'), 38, 10) is null and 
                    src:kptm is not null) THEN
                    'kptm contains a non-numeric value : \'' || AS_VARCHAR(src:kptm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) THEN
                    'ncmp contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padr_ref_compnr), '0'), 38, 10) is null and 
                    src:padr_ref_compnr is not null) THEN
                    'padr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:padr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:psdt), '1900-01-01')) is null and 
                    src:psdt is not null) THEN
                    'psdt contains a non-timestamp value : \'' || AS_VARCHAR(src:psdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psta), '0'), 38, 10) is null and 
                    src:psta is not null) THEN
                    'psta contains a non-numeric value : \'' || AS_VARCHAR(src:psta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcs), '0'), 38, 10) is null and 
                    src:ptcs is not null) THEN
                    'ptcs contains a non-numeric value : \'' || AS_VARCHAR(src:ptcs) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rept), '0'), 38, 10) is null and 
                    src:rept is not null) THEN
                    'rept contains a non-numeric value : \'' || AS_VARCHAR(src:rept) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rers), '0'), 38, 10) is null and 
                    src:rers is not null) THEN
                    'rers contains a non-numeric value : \'' || AS_VARCHAR(src:rers) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scun), '0'), 38, 10) is null and 
                    src:scun is not null) THEN
                    'scun contains a non-numeric value : \'' || AS_VARCHAR(src:scun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbdg), '0'), 38, 10) is null and 
                    src:tbdg is not null) THEN
                    'tbdg contains a non-numeric value : \'' || AS_VARCHAR(src:tbdg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmpl), '0'), 38, 10) is null and 
                    src:tmpl is not null) THEN
                    'tmpl contains a non-numeric value : \'' || AS_VARCHAR(src:tmpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) THEN
                    'txtn contains a non-numeric value : \'' || AS_VARCHAR(src:txtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) THEN
                    'txtn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtn_ref_compnr) || '\'' WHEN 
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
                src:comp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM790 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmp), '0'), 38, 10) is null and 
                    src:acmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agty), '0'), 38, 10) is null and 
                    src:agty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arch), '0'), 38, 10) is null and 
                    src:arch is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ardt), '1900-01-01')) is null and 
                    src:ardt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmud_ref_compnr), '0'), 38, 10) is null and 
                    src:cmud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cobs_ref_compnr), '0'), 38, 10) is null and 
                    src:cobs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espt), '0'), 38, 10) is null and 
                    src:espt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fddt), '1900-01-01')) is null and 
                    src:fddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intp), '0'), 38, 10) is null and 
                    src:intp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipin), '0'), 38, 10) is null and 
                    src:ipin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kopr), '0'), 38, 10) is null and 
                    src:kopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kptm), '0'), 38, 10) is null and 
                    src:kptm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padr_ref_compnr), '0'), 38, 10) is null and 
                    src:padr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:psdt), '1900-01-01')) is null and 
                    src:psdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psta), '0'), 38, 10) is null and 
                    src:psta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcs), '0'), 38, 10) is null and 
                    src:ptcs is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rept), '0'), 38, 10) is null and 
                    src:rept is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rers), '0'), 38, 10) is null and 
                    src:rers is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scun), '0'), 38, 10) is null and 
                    src:scun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbdg), '0'), 38, 10) is null and 
                    src:tbdg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmpl), '0'), 38, 10) is null and 
                    src:tmpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)