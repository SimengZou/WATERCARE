CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM000_ERROR AS SELECT src, 'LN_TPPDM000' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:achs), '0'), 38, 10) is null and 
                    src:achs is not null) THEN
                    'achs contains a non-numeric value : \'' || AS_VARCHAR(src:achs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acus), '0'), 38, 10) is null and 
                    src:acus is not null) THEN
                    'acus contains a non-numeric value : \'' || AS_VARCHAR(src:acus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bghi), '0'), 38, 10) is null and 
                    src:bghi is not null) THEN
                    'bghi contains a non-numeric value : \'' || AS_VARCHAR(src:bghi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgnw), '0'), 38, 10) is null and 
                    src:bgnw is not null) THEN
                    'bgnw contains a non-numeric value : \'' || AS_VARCHAR(src:bgnw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacc), '0'), 38, 10) is null and 
                    src:cacc is not null) THEN
                    'cacc contains a non-numeric value : \'' || AS_VARCHAR(src:cacc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cact), '0'), 38, 10) is null and 
                    src:cact is not null) THEN
                    'cact contains a non-numeric value : \'' || AS_VARCHAR(src:cact) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacu), '0'), 38, 10) is null and 
                    src:cacu is not null) THEN
                    'cacu contains a non-numeric value : \'' || AS_VARCHAR(src:cacu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp_ref_compnr), '0'), 38, 10) is null and 
                    src:capp_ref_compnr is not null) THEN
                    'capp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:capp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccpr), '0'), 38, 10) is null and 
                    src:ccpr is not null) THEN
                    'ccpr contains a non-numeric value : \'' || AS_VARCHAR(src:ccpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccus), '0'), 38, 10) is null and 
                    src:ccus is not null) THEN
                    'ccus contains a non-numeric value : \'' || AS_VARCHAR(src:ccus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_acnl), '0'), 38, 10) is null and 
                    src:cdf_acnl is not null) THEN
                    'cdf_acnl contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_acnl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ocrl), '0'), 38, 10) is null and 
                    src:cdf_ocrl is not null) THEN
                    'cdf_ocrl contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_ocrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_scrl), '0'), 38, 10) is null and 
                    src:cdf_scrl is not null) THEN
                    'cdf_scrl contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_scrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgch), '0'), 38, 10) is null and 
                    src:cgch is not null) THEN
                    'cgch contains a non-numeric value : \'' || AS_VARCHAR(src:cgch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) THEN
                    'citg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpac), '0'), 38, 10) is null and 
                    src:cpac is not null) THEN
                    'cpac contains a non-numeric value : \'' || AS_VARCHAR(src:cpac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprc), '0'), 38, 10) is null and 
                    src:cprc is not null) THEN
                    'cprc contains a non-numeric value : \'' || AS_VARCHAR(src:cprc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprt), '0'), 38, 10) is null and 
                    src:cprt is not null) THEN
                    'cprt contains a non-numeric value : \'' || AS_VARCHAR(src:cprt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpru), '0'), 38, 10) is null and 
                    src:cpru is not null) THEN
                    'cpru contains a non-numeric value : \'' || AS_VARCHAR(src:cpru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpsp), '0'), 38, 10) is null and 
                    src:cpsp is not null) THEN
                    'cpsp contains a non-numeric value : \'' || AS_VARCHAR(src:cpsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpst), '0'), 38, 10) is null and 
                    src:cpst is not null) THEN
                    'cpst contains a non-numeric value : \'' || AS_VARCHAR(src:cpst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is null and 
                    src:crtp_ref_compnr is not null) THEN
                    'crtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspc), '0'), 38, 10) is null and 
                    src:cspc is not null) THEN
                    'cspc contains a non-numeric value : \'' || AS_VARCHAR(src:cspc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) THEN
                    'cspt contains a non-numeric value : \'' || AS_VARCHAR(src:cspt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspu), '0'), 38, 10) is null and 
                    src:cspu is not null) THEN
                    'cspu contains a non-numeric value : \'' || AS_VARCHAR(src:cspu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstc), '0'), 38, 10) is null and 
                    src:cstc is not null) THEN
                    'cstc contains a non-numeric value : \'' || AS_VARCHAR(src:cstc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstt), '0'), 38, 10) is null and 
                    src:cstt is not null) THEN
                    'cstt contains a non-numeric value : \'' || AS_VARCHAR(src:cstt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstu), '0'), 38, 10) is null and 
                    src:cstu is not null) THEN
                    'cstu contains a non-numeric value : \'' || AS_VARCHAR(src:cstu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctut_ref_compnr), '0'), 38, 10) is null and 
                    src:ctut_ref_compnr is not null) THEN
                    'ctut_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctut_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpca_ref_compnr), '0'), 38, 10) is null and 
                    src:dpca_ref_compnr is not null) THEN
                    'dpca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dpca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpch_ref_compnr), '0'), 38, 10) is null and 
                    src:dpch_ref_compnr is not null) THEN
                    'dpch_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dpch_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpci_ref_compnr), '0'), 38, 10) is null and 
                    src:dpci_ref_compnr is not null) THEN
                    'dpci_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dpci_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:egrp_essr_ref_compnr), '0'), 38, 10) is null and 
                    src:egrp_essr_ref_compnr is not null) THEN
                    'egrp_essr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:egrp_essr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elhs), '0'), 38, 10) is null and 
                    src:elhs is not null) THEN
                    'elhs contains a non-numeric value : \'' || AS_VARCHAR(src:elhs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elus), '0'), 38, 10) is null and 
                    src:elus is not null) THEN
                    'elus contains a non-numeric value : \'' || AS_VARCHAR(src:elus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exus), '0'), 38, 10) is null and 
                    src:exus is not null) THEN
                    'exus contains a non-numeric value : \'' || AS_VARCHAR(src:exus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frvt), '0'), 38, 10) is null and 
                    src:frvt is not null) THEN
                    'frvt contains a non-numeric value : \'' || AS_VARCHAR(src:frvt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccc), '0'), 38, 10) is null and 
                    src:lccc is not null) THEN
                    'lccc contains a non-numeric value : \'' || AS_VARCHAR(src:lccc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lexp), '0'), 38, 10) is null and 
                    src:lexp is not null) THEN
                    'lexp contains a non-numeric value : \'' || AS_VARCHAR(src:lexp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ests_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ests_ref_compnr is not null) THEN
                    'ngrp_ests_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrp_ests_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_prsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_prsr_ref_compnr is not null) THEN
                    'ngrp_prsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrp_prsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ref_compnr is not null) THEN
                    'ngrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlci), '0'), 38, 10) is null and 
                    src:nlci is not null) THEN
                    'nlci contains a non-numeric value : \'' || AS_VARCHAR(src:nlci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nleq), '0'), 38, 10) is null and 
                    src:nleq is not null) THEN
                    'nleq contains a non-numeric value : \'' || AS_VARCHAR(src:nleq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlit), '0'), 38, 10) is null and 
                    src:nlit is not null) THEN
                    'nlit contains a non-numeric value : \'' || AS_VARCHAR(src:nlit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlsb), '0'), 38, 10) is null and 
                    src:nlsb is not null) THEN
                    'nlsb contains a non-numeric value : \'' || AS_VARCHAR(src:nlsb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlta), '0'), 38, 10) is null and 
                    src:nlta is not null) THEN
                    'nlta contains a non-numeric value : \'' || AS_VARCHAR(src:nlta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohac), '0'), 38, 10) is null and 
                    src:ohac is not null) THEN
                    'ohac contains a non-numeric value : \'' || AS_VARCHAR(src:ohac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohhc), '0'), 38, 10) is null and 
                    src:ohhc is not null) THEN
                    'ohhc contains a non-numeric value : \'' || AS_VARCHAR(src:ohhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohim), '0'), 38, 10) is null and 
                    src:ohim is not null) THEN
                    'ohim contains a non-numeric value : \'' || AS_VARCHAR(src:ohim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) THEN
                    'pcmp contains a non-numeric value : \'' || AS_VARCHAR(src:pcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsp), '0'), 38, 10) is null and 
                    src:pcsp is not null) THEN
                    'pcsp contains a non-numeric value : \'' || AS_VARCHAR(src:pcsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc), '0'), 38, 10) is null and 
                    src:plmc is not null) THEN
                    'plmc contains a non-numeric value : \'' || AS_VARCHAR(src:plmc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc_ref_compnr), '0'), 38, 10) is null and 
                    src:plmc_ref_compnr is not null) THEN
                    'plmc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plmc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmi), '0'), 38, 10) is null and 
                    src:plmi is not null) THEN
                    'plmi contains a non-numeric value : \'' || AS_VARCHAR(src:plmi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbs), '0'), 38, 10) is null and 
                    src:prbs is not null) THEN
                    'prbs contains a non-numeric value : \'' || AS_VARCHAR(src:prbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prhs), '0'), 38, 10) is null and 
                    src:prhs is not null) THEN
                    'prhs contains a non-numeric value : \'' || AS_VARCHAR(src:prhs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) THEN
                    'pwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcap_ref_compnr), '0'), 38, 10) is null and 
                    src:rcap_ref_compnr is not null) THEN
                    'rcap_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcap_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcfe_ref_compnr), '0'), 38, 10) is null and 
                    src:rcfe_ref_compnr is not null) THEN
                    'rcfe_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcfe_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcpp_ref_compnr), '0'), 38, 10) is null and 
                    src:rcpp_ref_compnr is not null) THEN
                    'rcpp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcpp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rcpt_ref_compnr is not null) THEN
                    'rcpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtp_ref_compnr), '0'), 38, 10) is null and 
                    src:rrtp_ref_compnr is not null) THEN
                    'rrtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rrtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtus), '0'), 38, 10) is null and 
                    src:rtus is not null) THEN
                    'rtus contains a non-numeric value : \'' || AS_VARCHAR(src:rtus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_ref_compnr is not null) THEN
                    'tgrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tgrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_tpsr_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_tpsr_ref_compnr is not null) THEN
                    'tgrp_tpsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tgrp_tpsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpte_ref_compnr), '0'), 38, 10) is null and 
                    src:tpte_ref_compnr is not null) THEN
                    'tpte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upra), '0'), 38, 10) is null and 
                    src:upra is not null) THEN
                    'upra contains a non-numeric value : \'' || AS_VARCHAR(src:upra) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) THEN
                    'uset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usev), '0'), 38, 10) is null and 
                    src:usev is not null) THEN
                    'usev contains a non-numeric value : \'' || AS_VARCHAR(src:usev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwka), '0'), 38, 10) is null and 
                    src:uwka is not null) THEN
                    'uwka contains a non-numeric value : \'' || AS_VARCHAR(src:uwka) || '\'' WHEN 
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
                                    
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:achs), '0'), 38, 10) is null and 
                    src:achs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acus), '0'), 38, 10) is null and 
                    src:acus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bghi), '0'), 38, 10) is null and 
                    src:bghi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgnw), '0'), 38, 10) is null and 
                    src:bgnw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacc), '0'), 38, 10) is null and 
                    src:cacc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cact), '0'), 38, 10) is null and 
                    src:cact is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacu), '0'), 38, 10) is null and 
                    src:cacu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp_ref_compnr), '0'), 38, 10) is null and 
                    src:capp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccpr), '0'), 38, 10) is null and 
                    src:ccpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccus), '0'), 38, 10) is null and 
                    src:ccus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_acnl), '0'), 38, 10) is null and 
                    src:cdf_acnl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ocrl), '0'), 38, 10) is null and 
                    src:cdf_ocrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_scrl), '0'), 38, 10) is null and 
                    src:cdf_scrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgch), '0'), 38, 10) is null and 
                    src:cgch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpac), '0'), 38, 10) is null and 
                    src:cpac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprc), '0'), 38, 10) is null and 
                    src:cprc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprt), '0'), 38, 10) is null and 
                    src:cprt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpru), '0'), 38, 10) is null and 
                    src:cpru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpsp), '0'), 38, 10) is null and 
                    src:cpsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpst), '0'), 38, 10) is null and 
                    src:cpst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is null and 
                    src:crtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspc), '0'), 38, 10) is null and 
                    src:cspc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspu), '0'), 38, 10) is null and 
                    src:cspu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstc), '0'), 38, 10) is null and 
                    src:cstc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstt), '0'), 38, 10) is null and 
                    src:cstt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstu), '0'), 38, 10) is null and 
                    src:cstu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctut_ref_compnr), '0'), 38, 10) is null and 
                    src:ctut_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpca_ref_compnr), '0'), 38, 10) is null and 
                    src:dpca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpch_ref_compnr), '0'), 38, 10) is null and 
                    src:dpch_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpci_ref_compnr), '0'), 38, 10) is null and 
                    src:dpci_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:egrp_essr_ref_compnr), '0'), 38, 10) is null and 
                    src:egrp_essr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elhs), '0'), 38, 10) is null and 
                    src:elhs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elus), '0'), 38, 10) is null and 
                    src:elus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exus), '0'), 38, 10) is null and 
                    src:exus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frvt), '0'), 38, 10) is null and 
                    src:frvt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccc), '0'), 38, 10) is null and 
                    src:lccc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lexp), '0'), 38, 10) is null and 
                    src:lexp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ests_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ests_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_prsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_prsr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlci), '0'), 38, 10) is null and 
                    src:nlci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nleq), '0'), 38, 10) is null and 
                    src:nleq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlit), '0'), 38, 10) is null and 
                    src:nlit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlsb), '0'), 38, 10) is null and 
                    src:nlsb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nlta), '0'), 38, 10) is null and 
                    src:nlta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohac), '0'), 38, 10) is null and 
                    src:ohac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohhc), '0'), 38, 10) is null and 
                    src:ohhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohim), '0'), 38, 10) is null and 
                    src:ohim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsp), '0'), 38, 10) is null and 
                    src:pcsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc), '0'), 38, 10) is null and 
                    src:plmc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc_ref_compnr), '0'), 38, 10) is null and 
                    src:plmc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmi), '0'), 38, 10) is null and 
                    src:plmi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbs), '0'), 38, 10) is null and 
                    src:prbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prhs), '0'), 38, 10) is null and 
                    src:prhs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcap_ref_compnr), '0'), 38, 10) is null and 
                    src:rcap_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcfe_ref_compnr), '0'), 38, 10) is null and 
                    src:rcfe_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcpp_ref_compnr), '0'), 38, 10) is null and 
                    src:rcpp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rcpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtp_ref_compnr), '0'), 38, 10) is null and 
                    src:rrtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtus), '0'), 38, 10) is null and 
                    src:rtus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_tpsr_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_tpsr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpte_ref_compnr), '0'), 38, 10) is null and 
                    src:tpte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upra), '0'), 38, 10) is null and 
                    src:upra is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usev), '0'), 38, 10) is null and 
                    src:usev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwka), '0'), 38, 10) is null and 
                    src:uwka is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)