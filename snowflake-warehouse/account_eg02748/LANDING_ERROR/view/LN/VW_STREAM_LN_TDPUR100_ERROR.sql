CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR100_ERROR AS SELECT src, 'LN_TDPUR100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) THEN
                    'ccon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cneg), '0'), 38, 10) is null and 
                    src:cneg is not null) THEN
                    'cneg contains a non-numeric value : \'' || AS_VARCHAR(src:cneg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) THEN
                    'crcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cset_ref_compnr), '0'), 38, 10) is null and 
                    src:cset_ref_compnr is not null) THEN
                    'cset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) THEN
                    'ctcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) THEN
                    'ddat contains a non-timestamp value : \'' || AS_VARCHAR(src:ddat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) THEN
                    'edat contains a non-timestamp value : \'' || AS_VARCHAR(src:edat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enlh), '0'), 38, 10) is null and 
                    src:enlh is not null) THEN
                    'enlh contains a non-numeric value : \'' || AS_VARCHAR(src:enlh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) THEN
                    'etpc contains a non-numeric value : \'' || AS_VARCHAR(src:etpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:negt), '0'), 38, 10) is null and 
                    src:negt is not null) THEN
                    'negt contains a non-numeric value : \'' || AS_VARCHAR(src:negt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:qdat), '1900-01-01')) is null and 
                    src:qdat is not null) THEN
                    'qdat contains a non-timestamp value : \'' || AS_VARCHAR(src:qdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspa), '0'), 38, 10) is null and 
                    src:qspa is not null) THEN
                    'qspa contains a non-numeric value : \'' || AS_VARCHAR(src:qspa) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfqt_ref_compnr), '0'), 38, 10) is null and 
                    src:rfqt_ref_compnr is not null) THEN
                    'rfqt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rfqt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) THEN
                    'rtdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rtdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_ref_compnr is not null) THEN
                    'tpbk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpbk_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ulcr), '0'), 38, 10) is null and 
                    src:ulcr is not null) THEN
                    'ulcr contains a non-numeric value : \'' || AS_VARCHAR(src:ulcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uptc), '0'), 38, 10) is null and 
                    src:uptc is not null) THEN
                    'uptc contains a non-numeric value : \'' || AS_VARCHAR(src:uptc) || '\'' WHEN 
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
                                    
                src:qono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cneg), '0'), 38, 10) is null and 
                    src:cneg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is null and 
                    src:crcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cset_ref_compnr), '0'), 38, 10) is null and 
                    src:cset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is null and 
                    src:ctcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enlh), '0'), 38, 10) is null and 
                    src:enlh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:negt), '0'), 38, 10) is null and 
                    src:negt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:qdat), '1900-01-01')) is null and 
                    src:qdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspa), '0'), 38, 10) is null and 
                    src:qspa is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfqt_ref_compnr), '0'), 38, 10) is null and 
                    src:rfqt_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rtdt), '1900-01-01')) is null and 
                    src:rtdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ulcr), '0'), 38, 10) is null and 
                    src:ulcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uptc), '0'), 38, 10) is null and 
                    src:uptc is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)