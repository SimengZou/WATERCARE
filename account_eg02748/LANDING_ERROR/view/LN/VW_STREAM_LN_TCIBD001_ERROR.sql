CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCIBD001_ERROR AS SELECT src, 'LN_TCIBD001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is null and 
                    src:ccde_ref_compnr is not null) THEN
                    'ccde_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccde_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfg_ref_compnr), '0'), 38, 10) is null and 
                    src:ccfg_ref_compnr is not null) THEN
                    'ccfg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccfg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chma), '0'), 38, 10) is null and 
                    src:chma is not null) THEN
                    'chma contains a non-numeric value : \'' || AS_VARCHAR(src:chma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) THEN
                    'citg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cltr), '0'), 38, 10) is null and 
                    src:cltr is not null) THEN
                    'cltr contains a non-numeric value : \'' || AS_VARCHAR(src:cltr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnfg), '0'), 38, 10) is null and 
                    src:cnfg is not null) THEN
                    'cnfg contains a non-numeric value : \'' || AS_VARCHAR(src:cnfg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cntr_ref_compnr), '0'), 38, 10) is null and 
                    src:cntr_ref_compnr is not null) THEN
                    'cntr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cntr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coeu), '0'), 38, 10) is null and 
                    src:coeu is not null) THEN
                    'coeu contains a non-numeric value : \'' || AS_VARCHAR(src:coeu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cont), '0'), 38, 10) is null and 
                    src:cont is not null) THEN
                    'cont contains a non-numeric value : \'' || AS_VARCHAR(src:cont) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is null and 
                    src:cood_ref_compnr is not null) THEN
                    'cood_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cood_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcl_ref_compnr is not null) THEN
                    'cpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) THEN
                    'cpcp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is null and 
                    src:cpln_ref_compnr is not null) THEN
                    'cpln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) THEN
                    'cpva contains a non-numeric value : \'' || AS_VARCHAR(src:cpva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva_rcmp), '0'), 38, 10) is null and 
                    src:cpva_rcmp is not null) THEN
                    'cpva_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cpva_rcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csel_ref_compnr), '0'), 38, 10) is null and 
                    src:csel_ref_compnr is not null) THEN
                    'csel_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csel_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csig_ref_compnr), '0'), 38, 10) is null and 
                    src:csig_ref_compnr is not null) THEN
                    'csig_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csig_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) THEN
                    'ctyo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) THEN
                    'ctyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cust), '0'), 38, 10) is null and 
                    src:cust is not null) THEN
                    'cust contains a non-numeric value : \'' || AS_VARCHAR(src:cust) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) THEN
                    'cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfit_ref_compnr), '0'), 38, 10) is null and 
                    src:dfit_ref_compnr is not null) THEN
                    'dfit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpcr), '0'), 38, 10) is null and 
                    src:dpcr is not null) THEN
                    'dpcr contains a non-numeric value : \'' || AS_VARCHAR(src:dpcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpeg), '0'), 38, 10) is null and 
                    src:dpeg is not null) THEN
                    'dpeg contains a non-numeric value : \'' || AS_VARCHAR(src:dpeg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptp), '0'), 38, 10) is null and 
                    src:dptp is not null) THEN
                    'dptp contains a non-numeric value : \'' || AS_VARCHAR(src:dptp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpuu), '0'), 38, 10) is null and 
                    src:dpuu is not null) THEN
                    'dpuu contains a non-numeric value : \'' || AS_VARCHAR(src:dpuu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsct), '0'), 38, 10) is null and 
                    src:dsct is not null) THEN
                    'dsct contains a non-numeric value : \'' || AS_VARCHAR(src:dsct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edco), '0'), 38, 10) is null and 
                    src:edco is not null) THEN
                    'edco contains a non-numeric value : \'' || AS_VARCHAR(src:edco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efpr), '0'), 38, 10) is null and 
                    src:efpr is not null) THEN
                    'efpr contains a non-numeric value : \'' || AS_VARCHAR(src:efpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eitm), '0'), 38, 10) is null and 
                    src:eitm is not null) THEN
                    'eitm contains a non-numeric value : \'' || AS_VARCHAR(src:eitm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elcm), '0'), 38, 10) is null and 
                    src:elcm is not null) THEN
                    'elcm contains a non-numeric value : \'' || AS_VARCHAR(src:elcm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elrq), '0'), 38, 10) is null and 
                    src:elrq is not null) THEN
                    'elrq contains a non-numeric value : \'' || AS_VARCHAR(src:elrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:envc), '0'), 38, 10) is null and 
                    src:envc is not null) THEN
                    'envc contains a non-numeric value : \'' || AS_VARCHAR(src:envc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstk), '0'), 38, 10) is null and 
                    src:fstk is not null) THEN
                    'fstk contains a non-numeric value : \'' || AS_VARCHAR(src:fstk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ichg), '0'), 38, 10) is null and 
                    src:ichg is not null) THEN
                    'ichg contains a non-numeric value : \'' || AS_VARCHAR(src:ichg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icsi), '0'), 38, 10) is null and 
                    src:icsi is not null) THEN
                    'icsi contains a non-numeric value : \'' || AS_VARCHAR(src:icsi) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ippg), '0'), 38, 10) is null and 
                    src:ippg is not null) THEN
                    'ippg contains a non-numeric value : \'' || AS_VARCHAR(src:ippg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itmt), '0'), 38, 10) is null and 
                    src:itmt is not null) THEN
                    'itmt contains a non-numeric value : \'' || AS_VARCHAR(src:itmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kitm), '0'), 38, 10) is null and 
                    src:kitm is not null) THEN
                    'kitm contains a non-numeric value : \'' || AS_VARCHAR(src:kitm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lctr), '0'), 38, 10) is null and 
                    src:lctr is not null) THEN
                    'lctr contains a non-numeric value : \'' || AS_VARCHAR(src:lctr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltct), '0'), 38, 10) is null and 
                    src:ltct is not null) THEN
                    'ltct contains a non-numeric value : \'' || AS_VARCHAR(src:ltct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcoa), '0'), 38, 10) is null and 
                    src:mcoa is not null) THEN
                    'mcoa contains a non-numeric value : \'' || AS_VARCHAR(src:mcoa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oktm), '0'), 38, 10) is null and 
                    src:oktm is not null) THEN
                    'oktm contains a non-numeric value : \'' || AS_VARCHAR(src:oktm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opol), '0'), 38, 10) is null and 
                    src:opol is not null) THEN
                    'opol contains a non-numeric value : \'' || AS_VARCHAR(src:opol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opts), '0'), 38, 10) is null and 
                    src:opts is not null) THEN
                    'opts contains a non-numeric value : \'' || AS_VARCHAR(src:opts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) THEN
                    'osys contains a non-numeric value : \'' || AS_VARCHAR(src:osys) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppeg), '0'), 38, 10) is null and 
                    src:ppeg is not null) THEN
                    'ppeg contains a non-numeric value : \'' || AS_VARCHAR(src:ppeg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppss), '0'), 38, 10) is null and 
                    src:ppss is not null) THEN
                    'ppss contains a non-numeric value : \'' || AS_VARCHAR(src:ppss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psiu), '0'), 38, 10) is null and 
                    src:psiu is not null) THEN
                    'psiu contains a non-numeric value : \'' || AS_VARCHAR(src:psiu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rdpt_ref_compnr is not null) THEN
                    'rdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:repl), '0'), 38, 10) is null and 
                    src:repl is not null) THEN
                    'repl contains a non-numeric value : \'' || AS_VARCHAR(src:repl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rnum), '0'), 38, 10) is null and 
                    src:rnum is not null) THEN
                    'rnum contains a non-numeric value : \'' || AS_VARCHAR(src:rnum) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvdi), '0'), 38, 10) is null and 
                    src:rvdi is not null) THEN
                    'rvdi contains a non-numeric value : \'' || AS_VARCHAR(src:rvdi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sayn), '0'), 38, 10) is null and 
                    src:sayn is not null) THEN
                    'sayn contains a non-numeric value : \'' || AS_VARCHAR(src:sayn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seri), '0'), 38, 10) is null and 
                    src:seri is not null) THEN
                    'seri contains a non-numeric value : \'' || AS_VARCHAR(src:seri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgtc), '0'), 38, 10) is null and 
                    src:sgtc is not null) THEN
                    'sgtc contains a non-numeric value : \'' || AS_VARCHAR(src:sgtc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srce), '0'), 38, 10) is null and 
                    src:srce is not null) THEN
                    'srce contains a non-numeric value : \'' || AS_VARCHAR(src:srce) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stoi), '0'), 38, 10) is null and 
                    src:stoi is not null) THEN
                    'stoi contains a non-numeric value : \'' || AS_VARCHAR(src:stoi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) THEN
                    'styp contains a non-numeric value : \'' || AS_VARCHAR(src:styp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uefs), '0'), 38, 10) is null and 
                    src:uefs is not null) THEN
                    'uefs contains a non-numeric value : \'' || AS_VARCHAR(src:uefs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umer), '0'), 38, 10) is null and 
                    src:umer is not null) THEN
                    'umer contains a non-numeric value : \'' || AS_VARCHAR(src:umer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unef), '0'), 38, 10) is null and 
                    src:unef is not null) THEN
                    'unef contains a non-numeric value : \'' || AS_VARCHAR(src:unef) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_cwun_ref_compnr is not null) THEN
                    'uset_cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) THEN
                    'uset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wpcs), '0'), 38, 10) is null and 
                    src:wpcs is not null) THEN
                    'wpcs contains a non-numeric value : \'' || AS_VARCHAR(src:wpcs) || '\'' WHEN 
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
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCIBD001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is null and 
                    src:ccde_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfg_ref_compnr), '0'), 38, 10) is null and 
                    src:ccfg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chma), '0'), 38, 10) is null and 
                    src:chma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cltr), '0'), 38, 10) is null and 
                    src:cltr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnfg), '0'), 38, 10) is null and 
                    src:cnfg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cntr_ref_compnr), '0'), 38, 10) is null and 
                    src:cntr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coeu), '0'), 38, 10) is null and 
                    src:coeu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cont), '0'), 38, 10) is null and 
                    src:cont is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is null and 
                    src:cood_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is null and 
                    src:cpln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva_rcmp), '0'), 38, 10) is null and 
                    src:cpva_rcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csel_ref_compnr), '0'), 38, 10) is null and 
                    src:csel_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csig_ref_compnr), '0'), 38, 10) is null and 
                    src:csig_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cust), '0'), 38, 10) is null and 
                    src:cust is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfit_ref_compnr), '0'), 38, 10) is null and 
                    src:dfit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpcr), '0'), 38, 10) is null and 
                    src:dpcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpeg), '0'), 38, 10) is null and 
                    src:dpeg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptp), '0'), 38, 10) is null and 
                    src:dptp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpuu), '0'), 38, 10) is null and 
                    src:dpuu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsct), '0'), 38, 10) is null and 
                    src:dsct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edco), '0'), 38, 10) is null and 
                    src:edco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efpr), '0'), 38, 10) is null and 
                    src:efpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eitm), '0'), 38, 10) is null and 
                    src:eitm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elcm), '0'), 38, 10) is null and 
                    src:elcm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elrq), '0'), 38, 10) is null and 
                    src:elrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:envc), '0'), 38, 10) is null and 
                    src:envc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstk), '0'), 38, 10) is null and 
                    src:fstk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ichg), '0'), 38, 10) is null and 
                    src:ichg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icsi), '0'), 38, 10) is null and 
                    src:icsi is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ippg), '0'), 38, 10) is null and 
                    src:ippg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itmt), '0'), 38, 10) is null and 
                    src:itmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kitm), '0'), 38, 10) is null and 
                    src:kitm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lctr), '0'), 38, 10) is null and 
                    src:lctr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltct), '0'), 38, 10) is null and 
                    src:ltct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcoa), '0'), 38, 10) is null and 
                    src:mcoa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oktm), '0'), 38, 10) is null and 
                    src:oktm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opol), '0'), 38, 10) is null and 
                    src:opol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opts), '0'), 38, 10) is null and 
                    src:opts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppeg), '0'), 38, 10) is null and 
                    src:ppeg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppss), '0'), 38, 10) is null and 
                    src:ppss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psiu), '0'), 38, 10) is null and 
                    src:psiu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:repl), '0'), 38, 10) is null and 
                    src:repl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rnum), '0'), 38, 10) is null and 
                    src:rnum is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rvdi), '0'), 38, 10) is null and 
                    src:rvdi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sayn), '0'), 38, 10) is null and 
                    src:sayn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seri), '0'), 38, 10) is null and 
                    src:seri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgtc), '0'), 38, 10) is null and 
                    src:sgtc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srce), '0'), 38, 10) is null and 
                    src:srce is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stoi), '0'), 38, 10) is null and 
                    src:stoi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uefs), '0'), 38, 10) is null and 
                    src:uefs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umer), '0'), 38, 10) is null and 
                    src:umer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unef), '0'), 38, 10) is null and 
                    src:unef is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_cwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wpcs), '0'), 38, 10) is null and 
                    src:wpcs is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)