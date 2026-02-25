CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM605_ERROR AS SELECT src, 'LN_TPPDM605' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:abpp), '0'), 38, 10) is null and 
                    src:abpp is not null) THEN
                    'abpp contains a non-numeric value : \'' || AS_VARCHAR(src:abpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:abrf), '0'), 38, 10) is null and 
                    src:abrf is not null) THEN
                    'abrf contains a non-numeric value : \'' || AS_VARCHAR(src:abrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acgm), '0'), 38, 10) is null and 
                    src:acgm is not null) THEN
                    'acgm contains a non-numeric value : \'' || AS_VARCHAR(src:acgm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmp), '0'), 38, 10) is null and 
                    src:acmp is not null) THEN
                    'acmp contains a non-numeric value : \'' || AS_VARCHAR(src:acmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpc), '0'), 38, 10) is null and 
                    src:acpc is not null) THEN
                    'acpc contains a non-numeric value : \'' || AS_VARCHAR(src:acpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aerf), '0'), 38, 10) is null and 
                    src:aerf is not null) THEN
                    'aerf contains a non-numeric value : \'' || AS_VARCHAR(src:aerf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agty), '0'), 38, 10) is null and 
                    src:agty is not null) THEN
                    'agty contains a non-numeric value : \'' || AS_VARCHAR(src:agty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) THEN
                    'aprp contains a non-numeric value : \'' || AS_VARCHAR(src:aprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arpc), '0'), 38, 10) is null and 
                    src:arpc is not null) THEN
                    'arpc contains a non-numeric value : \'' || AS_VARCHAR(src:arpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrl), '0'), 38, 10) is null and 
                    src:arrl is not null) THEN
                    'arrl contains a non-numeric value : \'' || AS_VARCHAR(src:arrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrm), '0'), 38, 10) is null and 
                    src:arrm is not null) THEN
                    'arrm contains a non-numeric value : \'' || AS_VARCHAR(src:arrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrt), '0'), 38, 10) is null and 
                    src:arrt is not null) THEN
                    'arrt contains a non-numeric value : \'' || AS_VARCHAR(src:arrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) THEN
                    'avtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:avtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdmt), '0'), 38, 10) is null and 
                    src:bdmt is not null) THEN
                    'bdmt contains a non-numeric value : \'' || AS_VARCHAR(src:bdmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfpp), '0'), 38, 10) is null and 
                    src:bfpp is not null) THEN
                    'bfpp contains a non-numeric value : \'' || AS_VARCHAR(src:bfpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfrf), '0'), 38, 10) is null and 
                    src:bfrf is not null) THEN
                    'bfrf contains a non-numeric value : \'' || AS_VARCHAR(src:bfrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btco), '0'), 38, 10) is null and 
                    src:btco is not null) THEN
                    'btco contains a non-numeric value : \'' || AS_VARCHAR(src:btco) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) THEN
                    'btdt contains a non-timestamp value : \'' || AS_VARCHAR(src:btdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btpr), '0'), 38, 10) is null and 
                    src:btpr is not null) THEN
                    'btpr contains a non-numeric value : \'' || AS_VARCHAR(src:btpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) THEN
                    'capp contains a non-numeric value : \'' || AS_VARCHAR(src:capp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) THEN
                    'carr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) THEN
                    'ccam_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccam_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ccdt), '1900-01-01')) is null and 
                    src:ccdt is not null) THEN
                    'ccdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ccdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) THEN
                    'ccot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) THEN
                    'cdis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) THEN
                    'cfac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfpr), '0'), 38, 10) is null and 
                    src:cfpr is not null) THEN
                    'cfpr contains a non-numeric value : \'' || AS_VARCHAR(src:cfpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfye), '0'), 38, 10) is null and 
                    src:cfye is not null) THEN
                    'cfye contains a non-numeric value : \'' || AS_VARCHAR(src:cfye) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cldr_ref_compnr), '0'), 38, 10) is null and 
                    src:cldr_ref_compnr is not null) THEN
                    'cldr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cldr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmog), '0'), 38, 10) is null and 
                    src:cmog is not null) THEN
                    'cmog contains a non-numeric value : \'' || AS_VARCHAR(src:cmog) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpc), '0'), 38, 10) is null and 
                    src:cmpc is not null) THEN
                    'cmpc contains a non-numeric value : \'' || AS_VARCHAR(src:cmpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmud_ref_compnr), '0'), 38, 10) is null and 
                    src:cmud_ref_compnr is not null) THEN
                    'cmud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cobs_ref_compnr), '0'), 38, 10) is null and 
                    src:cobs_ref_compnr is not null) THEN
                    'cobs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cobs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogc), '0'), 38, 10) is null and 
                    src:cogc is not null) THEN
                    'cogc contains a non-numeric value : \'' || AS_VARCHAR(src:cogc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogs), '0'), 38, 10) is null and 
                    src:cogs is not null) THEN
                    'cogs contains a non-numeric value : \'' || AS_VARCHAR(src:cogs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) THEN
                    'coty contains a non-numeric value : \'' || AS_VARCHAR(src:coty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) THEN
                    'cprj_ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_rsac_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_rsac_ref_compnr is not null) THEN
                    'cprj_cpla_rsac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_rsac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprq), '0'), 38, 10) is null and 
                    src:cprq is not null) THEN
                    'cprq contains a non-numeric value : \'' || AS_VARCHAR(src:cprq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is null and 
                    src:crtp_ref_compnr is not null) THEN
                    'crtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) THEN
                    'cstg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlau), '1900-01-01')) is null and 
                    src:dlau is not null) THEN
                    'dlau contains a non-timestamp value : \'' || AS_VARCHAR(src:dlau) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) THEN
                    'dldt contains a non-timestamp value : \'' || AS_VARCHAR(src:dldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) THEN
                    'dwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:earf), '0'), 38, 10) is null and 
                    src:earf is not null) THEN
                    'earf contains a non-numeric value : \'' || AS_VARCHAR(src:earf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espt), '0'), 38, 10) is null and 
                    src:espt is not null) THEN
                    'espt contains a non-numeric value : \'' || AS_VARCHAR(src:espt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fddt), '1900-01-01')) is null and 
                    src:fddt is not null) THEN
                    'fddt contains a non-timestamp value : \'' || AS_VARCHAR(src:fddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) THEN
                    'hbpc contains a non-numeric value : \'' || AS_VARCHAR(src:hbpc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:hdat), '1900-01-01')) is null and 
                    src:hdat is not null) THEN
                    'hdat contains a non-timestamp value : \'' || AS_VARCHAR(src:hdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hsts), '0'), 38, 10) is null and 
                    src:hsts is not null) THEN
                    'hsts contains a non-numeric value : \'' || AS_VARCHAR(src:hsts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:igcd), '0'), 38, 10) is null and 
                    src:igcd is not null) THEN
                    'igcd contains a non-numeric value : \'' || AS_VARCHAR(src:igcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intp), '0'), 38, 10) is null and 
                    src:intp is not null) THEN
                    'intp contains a non-numeric value : \'' || AS_VARCHAR(src:intp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) THEN
                    'invm contains a non-numeric value : \'' || AS_VARCHAR(src:invm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invr), '0'), 38, 10) is null and 
                    src:invr is not null) THEN
                    'invr contains a non-numeric value : \'' || AS_VARCHAR(src:invr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipin), '0'), 38, 10) is null and 
                    src:ipin is not null) THEN
                    'ipin contains a non-numeric value : \'' || AS_VARCHAR(src:ipin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kopr), '0'), 38, 10) is null and 
                    src:kopr is not null) THEN
                    'kopr contains a non-numeric value : \'' || AS_VARCHAR(src:kopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kptm), '0'), 38, 10) is null and 
                    src:kptm is not null) THEN
                    'kptm contains a non-numeric value : \'' || AS_VARCHAR(src:kptm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdt), '1900-01-01')) is null and 
                    src:lcdt is not null) THEN
                    'lcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lcdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lpdt), '1900-01-01')) is null and 
                    src:lpdt is not null) THEN
                    'lpdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lpdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrc), '1900-01-01')) is null and 
                    src:ltrc is not null) THEN
                    'ltrc contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrs), '1900-01-01')) is null and 
                    src:ltrs is not null) THEN
                    'ltrs contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwha), '0'), 38, 10) is null and 
                    src:lwha is not null) THEN
                    'lwha contains a non-numeric value : \'' || AS_VARCHAR(src:lwha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwhb), '0'), 38, 10) is null and 
                    src:lwhb is not null) THEN
                    'lwhb contains a non-numeric value : \'' || AS_VARCHAR(src:lwhb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwhc), '0'), 38, 10) is null and 
                    src:lwhc is not null) THEN
                    'lwhc contains a non-numeric value : \'' || AS_VARCHAR(src:lwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwhd), '0'), 38, 10) is null and 
                    src:lwhd is not null) THEN
                    'lwhd contains a non-numeric value : \'' || AS_VARCHAR(src:lwhd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwta), '0'), 38, 10) is null and 
                    src:lwta is not null) THEN
                    'lwta contains a non-numeric value : \'' || AS_VARCHAR(src:lwta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwtb), '0'), 38, 10) is null and 
                    src:lwtb is not null) THEN
                    'lwtb contains a non-numeric value : \'' || AS_VARCHAR(src:lwtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwtc), '0'), 38, 10) is null and 
                    src:lwtc is not null) THEN
                    'lwtc contains a non-numeric value : \'' || AS_VARCHAR(src:lwtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) THEN
                    'ncmp contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:njsp), '0'), 38, 10) is null and 
                    src:njsp is not null) THEN
                    'njsp contains a non-numeric value : \'' || AS_VARCHAR(src:njsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofes_ref_compnr), '0'), 38, 10) is null and 
                    src:ofes_ref_compnr is not null) THEN
                    'ofes_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofes_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofpr_ref_compnr), '0'), 38, 10) is null and 
                    src:ofpr_ref_compnr is not null) THEN
                    'ofpr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofpr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padr_ref_compnr), '0'), 38, 10) is null and 
                    src:padr_ref_compnr is not null) THEN
                    'padr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:padr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcba), '0'), 38, 10) is null and 
                    src:pcba is not null) THEN
                    'pcba contains a non-numeric value : \'' || AS_VARCHAR(src:pcba) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pceq), '0'), 38, 10) is null and 
                    src:pceq is not null) THEN
                    'pceq contains a non-numeric value : \'' || AS_VARCHAR(src:pceq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcic), '0'), 38, 10) is null and 
                    src:pcic is not null) THEN
                    'pcic contains a non-numeric value : \'' || AS_VARCHAR(src:pcic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcit), '0'), 38, 10) is null and 
                    src:pcit is not null) THEN
                    'pcit contains a non-numeric value : \'' || AS_VARCHAR(src:pcit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) THEN
                    'pcmp contains a non-numeric value : \'' || AS_VARCHAR(src:pcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pcrd), '1900-01-01')) is null and 
                    src:pcrd is not null) THEN
                    'pcrd contains a non-timestamp value : \'' || AS_VARCHAR(src:pcrd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsb), '0'), 38, 10) is null and 
                    src:pcsb is not null) THEN
                    'pcsb contains a non-numeric value : \'' || AS_VARCHAR(src:pcsb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcts), '0'), 38, 10) is null and 
                    src:pcts is not null) THEN
                    'pcts contains a non-numeric value : \'' || AS_VARCHAR(src:pcts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) THEN
                    'pcwa contains a non-numeric value : \'' || AS_VARCHAR(src:pcwa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plcd), '0'), 38, 10) is null and 
                    src:plcd is not null) THEN
                    'plcd contains a non-numeric value : \'' || AS_VARCHAR(src:plcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plgr_ref_compnr), '0'), 38, 10) is null and 
                    src:plgr_ref_compnr is not null) THEN
                    'plgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmfc_ref_compnr), '0'), 38, 10) is null and 
                    src:pmfc_ref_compnr is not null) THEN
                    'pmfc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pmfc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmng_ref_compnr), '0'), 38, 10) is null and 
                    src:pmng_ref_compnr is not null) THEN
                    'pmng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pmng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potp_ref_compnr), '0'), 38, 10) is null and 
                    src:potp_ref_compnr is not null) THEN
                    'potp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:potp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:preq), '0'), 38, 10) is null and 
                    src:preq is not null) THEN
                    'preq contains a non-numeric value : \'' || AS_VARCHAR(src:preq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prgm_ref_compnr), '0'), 38, 10) is null and 
                    src:prgm_ref_compnr is not null) THEN
                    'prgm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prgm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prit), '0'), 38, 10) is null and 
                    src:prit is not null) THEN
                    'prit contains a non-numeric value : \'' || AS_VARCHAR(src:prit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pror), '0'), 38, 10) is null and 
                    src:pror is not null) THEN
                    'pror contains a non-numeric value : \'' || AS_VARCHAR(src:pror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpc), '0'), 38, 10) is null and 
                    src:prpc is not null) THEN
                    'prpc contains a non-numeric value : \'' || AS_VARCHAR(src:prpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) THEN
                    'prre contains a non-numeric value : \'' || AS_VARCHAR(src:prre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsb), '0'), 38, 10) is null and 
                    src:prsb is not null) THEN
                    'prsb contains a non-numeric value : \'' || AS_VARCHAR(src:prsb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prts), '0'), 38, 10) is null and 
                    src:prts is not null) THEN
                    'prts contains a non-numeric value : \'' || AS_VARCHAR(src:prts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psic), '0'), 38, 10) is null and 
                    src:psic is not null) THEN
                    'psic contains a non-numeric value : \'' || AS_VARCHAR(src:psic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psit), '0'), 38, 10) is null and 
                    src:psit is not null) THEN
                    'psit contains a non-numeric value : \'' || AS_VARCHAR(src:psit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pssc), '0'), 38, 10) is null and 
                    src:pssc is not null) THEN
                    'pssc contains a non-numeric value : \'' || AS_VARCHAR(src:pssc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psta), '0'), 38, 10) is null and 
                    src:psta is not null) THEN
                    'psta contains a non-numeric value : \'' || AS_VARCHAR(src:psta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pstf), '0'), 38, 10) is null and 
                    src:pstf is not null) THEN
                    'pstf contains a non-numeric value : \'' || AS_VARCHAR(src:pstf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) THEN
                    'psts contains a non-numeric value : \'' || AS_VARCHAR(src:psts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcs), '0'), 38, 10) is null and 
                    src:ptcs is not null) THEN
                    'ptcs contains a non-numeric value : \'' || AS_VARCHAR(src:ptcs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) THEN
                    'pwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) THEN
                    'ratc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) THEN
                    'rats contains a non-numeric value : \'' || AS_VARCHAR(src:rats) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is null and 
                    src:rcmc_1 is not null) THEN
                    'rcmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is null and 
                    src:rcmc_2 is not null) THEN
                    'rcmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is null and 
                    src:rcmc_3 is not null) THEN
                    'rcmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) THEN
                    'revl contains a non-numeric value : \'' || AS_VARCHAR(src:revl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) THEN
                    'revr contains a non-numeric value : \'' || AS_VARCHAR(src:revr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revt), '0'), 38, 10) is null and 
                    src:revt is not null) THEN
                    'revt contains a non-numeric value : \'' || AS_VARCHAR(src:revt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ripr_ref_compnr), '0'), 38, 10) is null and 
                    src:ripr_ref_compnr is not null) THEN
                    'ripr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ripr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rjct), '0'), 38, 10) is null and 
                    src:rjct is not null) THEN
                    'rjct contains a non-numeric value : \'' || AS_VARCHAR(src:rjct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtp_ref_compnr), '0'), 38, 10) is null and 
                    src:rrtp_ref_compnr is not null) THEN
                    'rrtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rrtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is null and 
                    src:rsmc_1 is not null) THEN
                    'rsmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is null and 
                    src:rsmc_2 is not null) THEN
                    'rsmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is null and 
                    src:rsmc_3 is not null) THEN
                    'rsmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsrv), '0'), 38, 10) is null and 
                    src:rsrv is not null) THEN
                    'rsrv contains a non-numeric value : \'' || AS_VARCHAR(src:rsrv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is null and 
                    src:sadr_ref_compnr is not null) THEN
                    'sadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stus), '0'), 38, 10) is null and 
                    src:stus is not null) THEN
                    'stus contains a non-numeric value : \'' || AS_VARCHAR(src:stus) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uobs), '0'), 38, 10) is null and 
                    src:uobs is not null) THEN
                    'uobs contains a non-numeric value : \'' || AS_VARCHAR(src:uobs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upal), '0'), 38, 10) is null and 
                    src:upal is not null) THEN
                    'upal contains a non-numeric value : \'' || AS_VARCHAR(src:upal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ussu), '0'), 38, 10) is null and 
                    src:ussu is not null) THEN
                    'ussu contains a non-numeric value : \'' || AS_VARCHAR(src:ussu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wipc), '0'), 38, 10) is null and 
                    src:wipc is not null) THEN
                    'wipc contains a non-numeric value : \'' || AS_VARCHAR(src:wipc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
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
                                    
                src:hdat,
                src:cprj,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM605 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:abpp), '0'), 38, 10) is null and 
                    src:abpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:abrf), '0'), 38, 10) is null and 
                    src:abrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acgm), '0'), 38, 10) is null and 
                    src:acgm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmp), '0'), 38, 10) is null and 
                    src:acmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpc), '0'), 38, 10) is null and 
                    src:acpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aerf), '0'), 38, 10) is null and 
                    src:aerf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agty), '0'), 38, 10) is null and 
                    src:agty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arpc), '0'), 38, 10) is null and 
                    src:arpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrl), '0'), 38, 10) is null and 
                    src:arrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrm), '0'), 38, 10) is null and 
                    src:arrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arrt), '0'), 38, 10) is null and 
                    src:arrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdmt), '0'), 38, 10) is null and 
                    src:bdmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfpp), '0'), 38, 10) is null and 
                    src:bfpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfrf), '0'), 38, 10) is null and 
                    src:bfrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btco), '0'), 38, 10) is null and 
                    src:btco is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:btdt), '1900-01-01')) is null and 
                    src:btdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btpr), '0'), 38, 10) is null and 
                    src:btpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is null and 
                    src:ccam_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ccdt), '1900-01-01')) is null and 
                    src:ccdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is null and 
                    src:ccot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is null and 
                    src:cfac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfpr), '0'), 38, 10) is null and 
                    src:cfpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfye), '0'), 38, 10) is null and 
                    src:cfye is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cldr_ref_compnr), '0'), 38, 10) is null and 
                    src:cldr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmog), '0'), 38, 10) is null and 
                    src:cmog is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpc), '0'), 38, 10) is null and 
                    src:cmpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmud_ref_compnr), '0'), 38, 10) is null and 
                    src:cmud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cobs_ref_compnr), '0'), 38, 10) is null and 
                    src:cobs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogc), '0'), 38, 10) is null and 
                    src:cogc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cogs), '0'), 38, 10) is null and 
                    src:cogs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coty), '0'), 38, 10) is null and 
                    src:coty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_rsac_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_rsac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprq), '0'), 38, 10) is null and 
                    src:cprq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is null and 
                    src:crtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlau), '1900-01-01')) is null and 
                    src:dlau is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:earf), '0'), 38, 10) is null and 
                    src:earf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espt), '0'), 38, 10) is null and 
                    src:espt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fddt), '1900-01-01')) is null and 
                    src:fddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbpc), '0'), 38, 10) is null and 
                    src:hbpc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:hdat), '1900-01-01')) is null and 
                    src:hdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hsts), '0'), 38, 10) is null and 
                    src:hsts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:igcd), '0'), 38, 10) is null and 
                    src:igcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intp), '0'), 38, 10) is null and 
                    src:intp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invm), '0'), 38, 10) is null and 
                    src:invm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invr), '0'), 38, 10) is null and 
                    src:invr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipin), '0'), 38, 10) is null and 
                    src:ipin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kopr), '0'), 38, 10) is null and 
                    src:kopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kptm), '0'), 38, 10) is null and 
                    src:kptm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdt), '1900-01-01')) is null and 
                    src:lcdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lpdt), '1900-01-01')) is null and 
                    src:lpdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrc), '1900-01-01')) is null and 
                    src:ltrc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrs), '1900-01-01')) is null and 
                    src:ltrs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwha), '0'), 38, 10) is null and 
                    src:lwha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwhb), '0'), 38, 10) is null and 
                    src:lwhb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwhc), '0'), 38, 10) is null and 
                    src:lwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwhd), '0'), 38, 10) is null and 
                    src:lwhd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwta), '0'), 38, 10) is null and 
                    src:lwta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwtb), '0'), 38, 10) is null and 
                    src:lwtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lwtc), '0'), 38, 10) is null and 
                    src:lwtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:njsp), '0'), 38, 10) is null and 
                    src:njsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofes_ref_compnr), '0'), 38, 10) is null and 
                    src:ofes_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofpr_ref_compnr), '0'), 38, 10) is null and 
                    src:ofpr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padr_ref_compnr), '0'), 38, 10) is null and 
                    src:padr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcba), '0'), 38, 10) is null and 
                    src:pcba is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pceq), '0'), 38, 10) is null and 
                    src:pceq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcic), '0'), 38, 10) is null and 
                    src:pcic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcit), '0'), 38, 10) is null and 
                    src:pcit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pcrd), '1900-01-01')) is null and 
                    src:pcrd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsb), '0'), 38, 10) is null and 
                    src:pcsb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcts), '0'), 38, 10) is null and 
                    src:pcts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcwa), '0'), 38, 10) is null and 
                    src:pcwa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plcd), '0'), 38, 10) is null and 
                    src:plcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plgr_ref_compnr), '0'), 38, 10) is null and 
                    src:plgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmfc_ref_compnr), '0'), 38, 10) is null and 
                    src:pmfc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmng_ref_compnr), '0'), 38, 10) is null and 
                    src:pmng_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potp_ref_compnr), '0'), 38, 10) is null and 
                    src:potp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:preq), '0'), 38, 10) is null and 
                    src:preq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prgm_ref_compnr), '0'), 38, 10) is null and 
                    src:prgm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prit), '0'), 38, 10) is null and 
                    src:prit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pror), '0'), 38, 10) is null and 
                    src:pror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpc), '0'), 38, 10) is null and 
                    src:prpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsb), '0'), 38, 10) is null and 
                    src:prsb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prts), '0'), 38, 10) is null and 
                    src:prts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psic), '0'), 38, 10) is null and 
                    src:psic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psit), '0'), 38, 10) is null and 
                    src:psit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pssc), '0'), 38, 10) is null and 
                    src:pssc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psta), '0'), 38, 10) is null and 
                    src:psta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pstf), '0'), 38, 10) is null and 
                    src:pstf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcs), '0'), 38, 10) is null and 
                    src:ptcs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is null and 
                    src:rcmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is null and 
                    src:rcmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is null and 
                    src:rcmc_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revt), '0'), 38, 10) is null and 
                    src:revt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ripr_ref_compnr), '0'), 38, 10) is null and 
                    src:ripr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rjct), '0'), 38, 10) is null and 
                    src:rjct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtp_ref_compnr), '0'), 38, 10) is null and 
                    src:rrtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is null and 
                    src:rsmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is null and 
                    src:rsmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is null and 
                    src:rsmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsrv), '0'), 38, 10) is null and 
                    src:rsrv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is null and 
                    src:sadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stus), '0'), 38, 10) is null and 
                    src:stus is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uobs), '0'), 38, 10) is null and 
                    src:uobs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upal), '0'), 38, 10) is null and 
                    src:upal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ussu), '0'), 38, 10) is null and 
                    src:ussu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wipc), '0'), 38, 10) is null and 
                    src:wipc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)