CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP240_ERROR AS SELECT src, 'LN_TFACP240' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) THEN
                    'amth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) THEN
                    'amth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) THEN
                    'amth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr), '0'), 38, 10) is null and 
                    src:bppr is not null) THEN
                    'bppr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) THEN
                    'bptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ciko), '0'), 38, 10) is null and 
                    src:ciko is not null) THEN
                    'ciko contains a non-numeric value : \'' || AS_VARCHAR(src:ciko) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cipo), '0'), 38, 10) is null and 
                    src:cipo is not null) THEN
                    'cipo contains a non-numeric value : \'' || AS_VARCHAR(src:cipo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cisq), '0'), 38, 10) is null and 
                    src:cisq is not null) THEN
                    'cisq contains a non-numeric value : \'' || AS_VARCHAR(src:cisq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is null and 
                    src:clas_ref_compnr is not null) THEN
                    'clas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clsb), '0'), 38, 10) is null and 
                    src:clsb is not null) THEN
                    'clsb contains a non-numeric value : \'' || AS_VARCHAR(src:clsb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) THEN
                    'crrf contains a non-numeric value : \'' || AS_VARCHAR(src:crrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvlo), '0'), 38, 10) is null and 
                    src:cvlo is not null) THEN
                    'cvlo contains a non-numeric value : \'' || AS_VARCHAR(src:cvlo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) THEN
                    'damt contains a non-numeric value : \'' || AS_VARCHAR(src:damt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcpo), '0'), 38, 10) is null and 
                    src:fcpo is not null) THEN
                    'fcpo contains a non-numeric value : \'' || AS_VARCHAR(src:fcpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) THEN
                    'fire contains a non-numeric value : \'' || AS_VARCHAR(src:fire) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsli), '0'), 38, 10) is null and 
                    src:fsli is not null) THEN
                    'fsli contains a non-numeric value : \'' || AS_VARCHAR(src:fsli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) THEN
                    'issp contains a non-numeric value : \'' || AS_VARCHAR(src:issp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itsc), '0'), 38, 10) is null and 
                    src:itsc is not null) THEN
                    'itsc contains a non-numeric value : \'' || AS_VARCHAR(src:itsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ityp), '0'), 38, 10) is null and 
                    src:ityp is not null) THEN
                    'ityp contains a non-numeric value : \'' || AS_VARCHAR(src:ityp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iuni_ref_compnr), '0'), 38, 10) is null and 
                    src:iuni_ref_compnr is not null) THEN
                    'iuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:iuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) THEN
                    'koor contains a non-numeric value : \'' || AS_VARCHAR(src:koor) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ladt), '1900-01-01')) is null and 
                    src:ladt is not null) THEN
                    'ladt contains a non-timestamp value : \'' || AS_VARCHAR(src:ladt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmat), '0'), 38, 10) is null and 
                    src:lmat is not null) THEN
                    'lmat contains a non-numeric value : \'' || AS_VARCHAR(src:lmat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) THEN
                    'loco contains a non-numeric value : \'' || AS_VARCHAR(src:loco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maam), '0'), 38, 10) is null and 
                    src:maam is not null) THEN
                    'maam contains a non-numeric value : \'' || AS_VARCHAR(src:maam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maqu), '0'), 38, 10) is null and 
                    src:maqu is not null) THEN
                    'maqu contains a non-numeric value : \'' || AS_VARCHAR(src:maqu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) THEN
                    'mcfr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcpr), '0'), 38, 10) is null and 
                    src:mcpr is not null) THEN
                    'mcpr contains a non-numeric value : \'' || AS_VARCHAR(src:mcpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mqpu), '0'), 38, 10) is null and 
                    src:mqpu is not null) THEN
                    'mqpu contains a non-numeric value : \'' || AS_VARCHAR(src:mqpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtch), '0'), 38, 10) is null and 
                    src:mtch is not null) THEN
                    'mtch contains a non-numeric value : \'' || AS_VARCHAR(src:mtch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) THEN
                    'oamt contains a non-numeric value : \'' || AS_VARCHAR(src:oamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) THEN
                    'odat contains a non-timestamp value : \'' || AS_VARCHAR(src:odat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:omti), '0'), 38, 10) is null and 
                    src:omti is not null) THEN
                    'omti contains a non-numeric value : \'' || AS_VARCHAR(src:omti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqan), '0'), 38, 10) is null and 
                    src:oqan is not null) THEN
                    'oqan contains a non-numeric value : \'' || AS_VARCHAR(src:oqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) THEN
                    'ortp contains a non-numeric value : \'' || AS_VARCHAR(src:ortp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) THEN
                    'oset contains a non-numeric value : \'' || AS_VARCHAR(src:oset) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) THEN
                    'otyp contains a non-numeric value : \'' || AS_VARCHAR(src:otyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) THEN
                    'paft contains a non-numeric value : \'' || AS_VARCHAR(src:paft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) THEN
                    'paya_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:paya_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppvp), '0'), 38, 10) is null and 
                    src:ppvp is not null) THEN
                    'ppvp contains a non-numeric value : \'' || AS_VARCHAR(src:ppvp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prrc), '0'), 38, 10) is null and 
                    src:prrc is not null) THEN
                    'prrc contains a non-numeric value : \'' || AS_VARCHAR(src:prrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptyp_ref_compnr is not null) THEN
                    'ptyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqty), '0'), 38, 10) is null and 
                    src:rqty is not null) THEN
                    'rqty contains a non-numeric value : \'' || AS_VARCHAR(src:rqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbl), '0'), 38, 10) is null and 
                    src:sfbl is not null) THEN
                    'sfbl contains a non-numeric value : \'' || AS_VARCHAR(src:sfbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slcp), '0'), 38, 10) is null and 
                    src:slcp is not null) THEN
                    'slcp contains a non-numeric value : \'' || AS_VARCHAR(src:slcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtp), '0'), 38, 10) is null and 
                    src:srtp is not null) THEN
                    'srtp contains a non-numeric value : \'' || AS_VARCHAR(src:srtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapq), '0'), 38, 10) is null and 
                    src:tapq is not null) THEN
                    'tapq contains a non-numeric value : \'' || AS_VARCHAR(src:tapq) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toma), '0'), 38, 10) is null and 
                    src:toma is not null) THEN
                    'toma contains a non-numeric value : \'' || AS_VARCHAR(src:toma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trqu), '0'), 38, 10) is null and 
                    src:trqu is not null) THEN
                    'trqu contains a non-numeric value : \'' || AS_VARCHAR(src:trqu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unit_ref_compnr), '0'), 38, 10) is null and 
                    src:unit_ref_compnr is not null) THEN
                    'unit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uppr), '0'), 38, 10) is null and 
                    src:uppr is not null) THEN
                    'uppr contains a non-numeric value : \'' || AS_VARCHAR(src:uppr) || '\'' WHEN 
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
                                    
                src:pono,
                src:loco,
                src:orno,
                src:otyp,
                src:compnr,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP240 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr), '0'), 38, 10) is null and 
                    src:bppr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is null and 
                    src:bptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ciko), '0'), 38, 10) is null and 
                    src:ciko is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cipo), '0'), 38, 10) is null and 
                    src:cipo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cisq), '0'), 38, 10) is null and 
                    src:cisq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is null and 
                    src:clas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clsb), '0'), 38, 10) is null and 
                    src:clsb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvlo), '0'), 38, 10) is null and 
                    src:cvlo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcpo), '0'), 38, 10) is null and 
                    src:fcpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsli), '0'), 38, 10) is null and 
                    src:fsli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itsc), '0'), 38, 10) is null and 
                    src:itsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ityp), '0'), 38, 10) is null and 
                    src:ityp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iuni_ref_compnr), '0'), 38, 10) is null and 
                    src:iuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ladt), '1900-01-01')) is null and 
                    src:ladt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmat), '0'), 38, 10) is null and 
                    src:lmat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maam), '0'), 38, 10) is null and 
                    src:maam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maqu), '0'), 38, 10) is null and 
                    src:maqu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcpr), '0'), 38, 10) is null and 
                    src:mcpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mqpu), '0'), 38, 10) is null and 
                    src:mqpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtch), '0'), 38, 10) is null and 
                    src:mtch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:omti), '0'), 38, 10) is null and 
                    src:omti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqan), '0'), 38, 10) is null and 
                    src:oqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppvp), '0'), 38, 10) is null and 
                    src:ppvp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prrc), '0'), 38, 10) is null and 
                    src:prrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptyp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqty), '0'), 38, 10) is null and 
                    src:rqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbl), '0'), 38, 10) is null and 
                    src:sfbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slcp), '0'), 38, 10) is null and 
                    src:slcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtp), '0'), 38, 10) is null and 
                    src:srtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapq), '0'), 38, 10) is null and 
                    src:tapq is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toma), '0'), 38, 10) is null and 
                    src:toma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trqu), '0'), 38, 10) is null and 
                    src:trqu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unit_ref_compnr), '0'), 38, 10) is null and 
                    src:unit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uppr), '0'), 38, 10) is null and 
                    src:uppr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)