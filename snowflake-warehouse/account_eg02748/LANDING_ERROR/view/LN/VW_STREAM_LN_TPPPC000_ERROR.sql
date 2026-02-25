CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC000_ERROR AS SELECT src, 'LN_TPPPC000' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) THEN
                    'amoc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor), '0'), 38, 10) is null and 
                    src:amor is not null) THEN
                    'amor contains a non-numeric value : \'' || AS_VARCHAR(src:amor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) THEN
                    'amos contains a non-numeric value : \'' || AS_VARCHAR(src:amos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:antp), '0'), 38, 10) is null and 
                    src:antp is not null) THEN
                    'antp contains a non-numeric value : \'' || AS_VARCHAR(src:antp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awpp), '0'), 38, 10) is null and 
                    src:awpp is not null) THEN
                    'awpp contains a non-numeric value : \'' || AS_VARCHAR(src:awpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casc_ref_compnr), '0'), 38, 10) is null and 
                    src:casc_ref_compnr is not null) THEN
                    'casc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casd_ref_compnr), '0'), 38, 10) is null and 
                    src:casd_ref_compnr is not null) THEN
                    'casd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccap), '0'), 38, 10) is null and 
                    src:ccap is not null) THEN
                    'ccap contains a non-numeric value : \'' || AS_VARCHAR(src:ccap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cint), '0'), 38, 10) is null and 
                    src:cint is not null) THEN
                    'cint contains a non-numeric value : \'' || AS_VARCHAR(src:cint) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmif), '0'), 38, 10) is null and 
                    src:cmif is not null) THEN
                    'cmif contains a non-numeric value : \'' || AS_VARCHAR(src:cmif) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprp), '0'), 38, 10) is null and 
                    src:cprp is not null) THEN
                    'cprp contains a non-numeric value : \'' || AS_VARCHAR(src:cprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_ref_compnr is not null) THEN
                    'cptc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) THEN
                    'cptc_year_peri_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptc_year_peri_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_ref_compnr is not null) THEN
                    'cptf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpth_hyea_hper_ref_compnr), '0'), 38, 10) is null and 
                    src:cpth_hyea_hper_ref_compnr is not null) THEN
                    'cpth_hyea_hper_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpth_hyea_hper_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpth_ref_compnr), '0'), 38, 10) is null and 
                    src:cpth_ref_compnr is not null) THEN
                    'cpth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcic_ref_compnr), '0'), 38, 10) is null and 
                    src:dcic_ref_compnr is not null) THEN
                    'dcic_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dcic_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dequ_ref_compnr), '0'), 38, 10) is null and 
                    src:dequ_ref_compnr is not null) THEN
                    'dequ_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dequ_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrt_ref_compnr), '0'), 38, 10) is null and 
                    src:dfrt_ref_compnr is not null) THEN
                    'dfrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dhlp_ref_compnr), '0'), 38, 10) is null and 
                    src:dhlp_ref_compnr is not null) THEN
                    'dhlp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dhlp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ditm_ref_compnr), '0'), 38, 10) is null and 
                    src:ditm_ref_compnr is not null) THEN
                    'ditm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ditm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsub_ref_compnr), '0'), 38, 10) is null and 
                    src:dsub_ref_compnr is not null) THEN
                    'dsub_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dsub_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrl_ref_compnr), '0'), 38, 10) is null and 
                    src:dtrl_ref_compnr is not null) THEN
                    'dtrl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dtrl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expc), '0'), 38, 10) is null and 
                    src:expc is not null) THEN
                    'expc contains a non-numeric value : \'' || AS_VARCHAR(src:expc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hper), '0'), 38, 10) is null and 
                    src:hper is not null) THEN
                    'hper contains a non-numeric value : \'' || AS_VARCHAR(src:hper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hyea), '0'), 38, 10) is null and 
                    src:hyea is not null) THEN
                    'hyea contains a non-numeric value : \'' || AS_VARCHAR(src:hyea) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipcs), '0'), 38, 10) is null and 
                    src:ipcs is not null) THEN
                    'ipcs contains a non-numeric value : \'' || AS_VARCHAR(src:ipcs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipiw), '0'), 38, 10) is null and 
                    src:ipiw is not null) THEN
                    'ipiw contains a non-numeric value : \'' || AS_VARCHAR(src:ipiw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lfcm), '0'), 38, 10) is null and 
                    src:lfcm is not null) THEN
                    'lfcm contains a non-numeric value : \'' || AS_VARCHAR(src:lfcm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcpr), '0'), 38, 10) is null and 
                    src:mcpr is not null) THEN
                    'mcpr contains a non-numeric value : \'' || AS_VARCHAR(src:mcpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfcr), '0'), 38, 10) is null and 
                    src:mfcr is not null) THEN
                    'mfcr contains a non-numeric value : \'' || AS_VARCHAR(src:mfcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pacc), '0'), 38, 10) is null and 
                    src:pacc is not null) THEN
                    'pacc contains a non-numeric value : \'' || AS_VARCHAR(src:pacc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfpp), '0'), 38, 10) is null and 
                    src:pfpp is not null) THEN
                    'pfpp contains a non-numeric value : \'' || AS_VARCHAR(src:pfpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgth), '0'), 38, 10) is null and 
                    src:pgth is not null) THEN
                    'pgth contains a non-numeric value : \'' || AS_VARCHAR(src:pgth) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppht), '0'), 38, 10) is null and 
                    src:ppht is not null) THEN
                    'ppht contains a non-numeric value : \'' || AS_VARCHAR(src:ppht) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prhr), '0'), 38, 10) is null and 
                    src:prhr is not null) THEN
                    'prhr contains a non-numeric value : \'' || AS_VARCHAR(src:prhr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qacc), '0'), 38, 10) is null and 
                    src:qacc is not null) THEN
                    'qacc contains a non-numeric value : \'' || AS_VARCHAR(src:qacc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) THEN
                    'revr contains a non-numeric value : \'' || AS_VARCHAR(src:revr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scev), '0'), 38, 10) is null and 
                    src:scev is not null) THEN
                    'scev contains a non-numeric value : \'' || AS_VARCHAR(src:scev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tohc), '0'), 38, 10) is null and 
                    src:tohc is not null) THEN
                    'tohc contains a non-numeric value : \'' || AS_VARCHAR(src:tohc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsem_ref_compnr), '0'), 38, 10) is null and 
                    src:tsem_ref_compnr is not null) THEN
                    'tsem_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tsem_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tstk_ref_compnr), '0'), 38, 10) is null and 
                    src:tstk_ref_compnr is not null) THEN
                    'tstk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tstk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upsa), '0'), 38, 10) is null and 
                    src:upsa is not null) THEN
                    'upsa contains a non-numeric value : \'' || AS_VARCHAR(src:upsa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urcc), '0'), 38, 10) is null and 
                    src:urcc is not null) THEN
                    'urcc contains a non-numeric value : \'' || AS_VARCHAR(src:urcc) || '\'' WHEN 
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
                                    
                src:compnr,
                src:indt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amor), '0'), 38, 10) is null and 
                    src:amor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amos), '0'), 38, 10) is null and 
                    src:amos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:antp), '0'), 38, 10) is null and 
                    src:antp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awpp), '0'), 38, 10) is null and 
                    src:awpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casc_ref_compnr), '0'), 38, 10) is null and 
                    src:casc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casd_ref_compnr), '0'), 38, 10) is null and 
                    src:casd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccap), '0'), 38, 10) is null and 
                    src:ccap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cint), '0'), 38, 10) is null and 
                    src:cint is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmif), '0'), 38, 10) is null and 
                    src:cmif is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprp), '0'), 38, 10) is null and 
                    src:cprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is null and 
                    src:cptc_year_peri_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is null and 
                    src:cptf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpth_hyea_hper_ref_compnr), '0'), 38, 10) is null and 
                    src:cpth_hyea_hper_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpth_ref_compnr), '0'), 38, 10) is null and 
                    src:cpth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcic_ref_compnr), '0'), 38, 10) is null and 
                    src:dcic_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dequ_ref_compnr), '0'), 38, 10) is null and 
                    src:dequ_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrt_ref_compnr), '0'), 38, 10) is null and 
                    src:dfrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dhlp_ref_compnr), '0'), 38, 10) is null and 
                    src:dhlp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ditm_ref_compnr), '0'), 38, 10) is null and 
                    src:ditm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsub_ref_compnr), '0'), 38, 10) is null and 
                    src:dsub_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrl_ref_compnr), '0'), 38, 10) is null and 
                    src:dtrl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expc), '0'), 38, 10) is null and 
                    src:expc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hper), '0'), 38, 10) is null and 
                    src:hper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hyea), '0'), 38, 10) is null and 
                    src:hyea is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipcs), '0'), 38, 10) is null and 
                    src:ipcs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipiw), '0'), 38, 10) is null and 
                    src:ipiw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lfcm), '0'), 38, 10) is null and 
                    src:lfcm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcpr), '0'), 38, 10) is null and 
                    src:mcpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfcr), '0'), 38, 10) is null and 
                    src:mfcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pacc), '0'), 38, 10) is null and 
                    src:pacc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfpp), '0'), 38, 10) is null and 
                    src:pfpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgth), '0'), 38, 10) is null and 
                    src:pgth is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppht), '0'), 38, 10) is null and 
                    src:ppht is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prhr), '0'), 38, 10) is null and 
                    src:prhr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qacc), '0'), 38, 10) is null and 
                    src:qacc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revr), '0'), 38, 10) is null and 
                    src:revr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scev), '0'), 38, 10) is null and 
                    src:scev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tohc), '0'), 38, 10) is null and 
                    src:tohc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsem_ref_compnr), '0'), 38, 10) is null and 
                    src:tsem_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tstk_ref_compnr), '0'), 38, 10) is null and 
                    src:tstk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upsa), '0'), 38, 10) is null and 
                    src:upsa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urcc), '0'), 38, 10) is null and 
                    src:urcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)