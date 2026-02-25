CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM110_ERROR AS SELECT src, 'LN_TFFAM110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpa_1), '0'), 38, 10) is null and 
                    src:adpa_1 is not null) THEN
                    'adpa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:adpa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpa_2), '0'), 38, 10) is null and 
                    src:adpa_2 is not null) THEN
                    'adpa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:adpa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpa_3), '0'), 38, 10) is null and 
                    src:adpa_3 is not null) THEN
                    'adpa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:adpa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpc), '0'), 38, 10) is null and 
                    src:adpc is not null) THEN
                    'adpc contains a non-numeric value : \'' || AS_VARCHAR(src:adpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpm_ref_compnr), '0'), 38, 10) is null and 
                    src:adpm_ref_compnr is not null) THEN
                    'adpm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:adpm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_ref_compnr is not null) THEN
                    'anbr_aext_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:anbr_aext_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is null and 
                    src:book_ref_compnr is not null) THEN
                    'book_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:book_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bslv), '0'), 38, 10) is null and 
                    src:bslv is not null) THEN
                    'bslv contains a non-numeric value : \'' || AS_VARCHAR(src:bslv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263_1), '0'), 38, 10) is null and 
                    src:c263_1 is not null) THEN
                    'c263_1 contains a non-numeric value : \'' || AS_VARCHAR(src:c263_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263_2), '0'), 38, 10) is null and 
                    src:c263_2 is not null) THEN
                    'c263_2 contains a non-numeric value : \'' || AS_VARCHAR(src:c263_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263_3), '0'), 38, 10) is null and 
                    src:c263_3 is not null) THEN
                    'c263_3 contains a non-numeric value : \'' || AS_VARCHAR(src:c263_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_1), '0'), 38, 10) is null and 
                    src:capi_1 is not null) THEN
                    'capi_1 contains a non-numeric value : \'' || AS_VARCHAR(src:capi_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_2), '0'), 38, 10) is null and 
                    src:capi_2 is not null) THEN
                    'capi_2 contains a non-numeric value : \'' || AS_VARCHAR(src:capi_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_3), '0'), 38, 10) is null and 
                    src:capi_3 is not null) THEN
                    'capi_3 contains a non-numeric value : \'' || AS_VARCHAR(src:capi_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_1), '0'), 38, 10) is null and 
                    src:cost_1 is not null) THEN
                    'cost_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cost_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_2), '0'), 38, 10) is null and 
                    src:cost_2 is not null) THEN
                    'cost_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cost_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_3), '0'), 38, 10) is null and 
                    src:cost_3 is not null) THEN
                    'cost_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cost_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:curd), '1900-01-01')) is null and 
                    src:curd is not null) THEN
                    'curd contains a non-timestamp value : \'' || AS_VARCHAR(src:curd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_1), '0'), 38, 10) is null and 
                    src:depr_1 is not null) THEN
                    'depr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_2), '0'), 38, 10) is null and 
                    src:depr_2 is not null) THEN
                    'depr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_3), '0'), 38, 10) is null and 
                    src:depr_3 is not null) THEN
                    'depr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:disd), '1900-01-01')) is null and 
                    src:disd is not null) THEN
                    'disd contains a non-timestamp value : \'' || AS_VARCHAR(src:disd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecre), '0'), 38, 10) is null and 
                    src:ecre is not null) THEN
                    'ecre contains a non-numeric value : \'' || AS_VARCHAR(src:ecre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq_ref_compnr), '0'), 38, 10) is null and 
                    src:freq_ref_compnr is not null) THEN
                    'freq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:freq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fydp), '0'), 38, 10) is null and 
                    src:fydp is not null) THEN
                    'fydp contains a non-numeric value : \'' || AS_VARCHAR(src:fydp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac_1), '0'), 38, 10) is null and 
                    src:gfac_1 is not null) THEN
                    'gfac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:gfac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac_2), '0'), 38, 10) is null and 
                    src:gfac_2 is not null) THEN
                    'gfac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:gfac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac_3), '0'), 38, 10) is null and 
                    src:gfac_3 is not null) THEN
                    'gfac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:gfac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:indx_ref_compnr), '0'), 38, 10) is null and 
                    src:indx_ref_compnr is not null) THEN
                    'indx_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:indx_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_1), '0'), 38, 10) is null and 
                    src:itca_1 is not null) THEN
                    'itca_1 contains a non-numeric value : \'' || AS_VARCHAR(src:itca_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_2), '0'), 38, 10) is null and 
                    src:itca_2 is not null) THEN
                    'itca_2 contains a non-numeric value : \'' || AS_VARCHAR(src:itca_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_3), '0'), 38, 10) is null and 
                    src:itca_3 is not null) THEN
                    'itca_3 contains a non-numeric value : \'' || AS_VARCHAR(src:itca_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:last), '1900-01-01')) is null and 
                    src:last is not null) THEN
                    'last contains a non-timestamp value : \'' || AS_VARCHAR(src:last) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) THEN
                    'life contains a non-numeric value : \'' || AS_VARCHAR(src:life) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lrev), '0'), 38, 10) is null and 
                    src:lrev is not null) THEN
                    'lrev contains a non-numeric value : \'' || AS_VARCHAR(src:lrev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltad_1), '0'), 38, 10) is null and 
                    src:ltad_1 is not null) THEN
                    'ltad_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ltad_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltad_2), '0'), 38, 10) is null and 
                    src:ltad_2 is not null) THEN
                    'ltad_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ltad_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltad_3), '0'), 38, 10) is null and 
                    src:ltad_3 is not null) THEN
                    'ltad_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ltad_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltar_1), '0'), 38, 10) is null and 
                    src:ltar_1 is not null) THEN
                    'ltar_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ltar_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltar_2), '0'), 38, 10) is null and 
                    src:ltar_2 is not null) THEN
                    'ltar_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ltar_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltar_3), '0'), 38, 10) is null and 
                    src:ltar_3 is not null) THEN
                    'ltar_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ltar_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_1), '0'), 38, 10) is null and 
                    src:ltdd_1 is not null) THEN
                    'ltdd_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_2), '0'), 38, 10) is null and 
                    src:ltdd_2 is not null) THEN
                    'ltdd_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_3), '0'), 38, 10) is null and 
                    src:ltdd_3 is not null) THEN
                    'ltdd_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdr_1), '0'), 38, 10) is null and 
                    src:ltdr_1 is not null) THEN
                    'ltdr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdr_2), '0'), 38, 10) is null and 
                    src:ltdr_2 is not null) THEN
                    'ltdr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdr_3), '0'), 38, 10) is null and 
                    src:ltdr_3 is not null) THEN
                    'ltdr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:meth_ref_compnr), '0'), 38, 10) is null and 
                    src:meth_ref_compnr is not null) THEN
                    'meth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:meth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_1), '0'), 38, 10) is null and 
                    src:ocst_1 is not null) THEN
                    'ocst_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ocst_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_2), '0'), 38, 10) is null and 
                    src:ocst_2 is not null) THEN
                    'ocst_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ocst_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_3), '0'), 38, 10) is null and 
                    src:ocst_3 is not null) THEN
                    'ocst_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ocst_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) THEN
                    'post contains a non-numeric value : \'' || AS_VARCHAR(src:post) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is null and 
                    src:prop_ref_compnr is not null) THEN
                    'prop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_1), '0'), 38, 10) is null and 
                    src:rcst_1 is not null) THEN
                    'rcst_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rcst_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_2), '0'), 38, 10) is null and 
                    src:rcst_2 is not null) THEN
                    'rcst_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rcst_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_3), '0'), 38, 10) is null and 
                    src:rcst_3 is not null) THEN
                    'rcst_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rcst_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpr_1), '0'), 38, 10) is null and 
                    src:rdpr_1 is not null) THEN
                    'rdpr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rdpr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpr_2), '0'), 38, 10) is null and 
                    src:rdpr_2 is not null) THEN
                    'rdpr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rdpr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpr_3), '0'), 38, 10) is null and 
                    src:rdpr_3 is not null) THEN
                    'rdpr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rdpr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_1), '0'), 38, 10) is null and 
                    src:s179_1 is not null) THEN
                    's179_1 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_2), '0'), 38, 10) is null and 
                    src:s179_2 is not null) THEN
                    's179_2 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_3), '0'), 38, 10) is null and 
                    src:s179_3 is not null) THEN
                    's179_3 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) THEN
                    'salv_1 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) THEN
                    'salv_2 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) THEN
                    'salv_3 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdpn), '0'), 38, 10) is null and 
                    src:sdpn is not null) THEN
                    'sdpn contains a non-numeric value : \'' || AS_VARCHAR(src:sdpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shft), '0'), 38, 10) is null and 
                    src:shft is not null) THEN
                    'shft contains a non-numeric value : \'' || AS_VARCHAR(src:shft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srac), '0'), 38, 10) is null and 
                    src:srac is not null) THEN
                    'srac contains a non-numeric value : \'' || AS_VARCHAR(src:srac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmth), '0'), 38, 10) is null and 
                    src:tmth is not null) THEN
                    'tmth contains a non-numeric value : \'' || AS_VARCHAR(src:tmth) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unit), '0'), 38, 10) is null and 
                    src:unit is not null) THEN
                    'unit contains a non-numeric value : \'' || AS_VARCHAR(src:unit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_1), '0'), 38, 10) is null and 
                    src:ytdd_1 is not null) THEN
                    'ytdd_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdd_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_2), '0'), 38, 10) is null and 
                    src:ytdd_2 is not null) THEN
                    'ytdd_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdd_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_3), '0'), 38, 10) is null and 
                    src:ytdd_3 is not null) THEN
                    'ytdd_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdd_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_1), '0'), 38, 10) is null and 
                    src:ytdr_1 is not null) THEN
                    'ytdr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_2), '0'), 38, 10) is null and 
                    src:ytdr_2 is not null) THEN
                    'ytdr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_3), '0'), 38, 10) is null and 
                    src:ytdr_3 is not null) THEN
                    'ytdr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdr_3) || '\'' WHEN 
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
                src:book,
                src:aext,
                src:anbr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpa_1), '0'), 38, 10) is null and 
                    src:adpa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpa_2), '0'), 38, 10) is null and 
                    src:adpa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpa_3), '0'), 38, 10) is null and 
                    src:adpa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpc), '0'), 38, 10) is null and 
                    src:adpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpm_ref_compnr), '0'), 38, 10) is null and 
                    src:adpm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is null and 
                    src:book_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bslv), '0'), 38, 10) is null and 
                    src:bslv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263_1), '0'), 38, 10) is null and 
                    src:c263_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263_2), '0'), 38, 10) is null and 
                    src:c263_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263_3), '0'), 38, 10) is null and 
                    src:c263_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_1), '0'), 38, 10) is null and 
                    src:capi_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_2), '0'), 38, 10) is null and 
                    src:capi_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_3), '0'), 38, 10) is null and 
                    src:capi_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_1), '0'), 38, 10) is null and 
                    src:cost_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_2), '0'), 38, 10) is null and 
                    src:cost_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_3), '0'), 38, 10) is null and 
                    src:cost_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:curd), '1900-01-01')) is null and 
                    src:curd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_1), '0'), 38, 10) is null and 
                    src:depr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_2), '0'), 38, 10) is null and 
                    src:depr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_3), '0'), 38, 10) is null and 
                    src:depr_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:disd), '1900-01-01')) is null and 
                    src:disd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecre), '0'), 38, 10) is null and 
                    src:ecre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq_ref_compnr), '0'), 38, 10) is null and 
                    src:freq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fydp), '0'), 38, 10) is null and 
                    src:fydp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac_1), '0'), 38, 10) is null and 
                    src:gfac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac_2), '0'), 38, 10) is null and 
                    src:gfac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac_3), '0'), 38, 10) is null and 
                    src:gfac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:indx_ref_compnr), '0'), 38, 10) is null and 
                    src:indx_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_1), '0'), 38, 10) is null and 
                    src:itca_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_2), '0'), 38, 10) is null and 
                    src:itca_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_3), '0'), 38, 10) is null and 
                    src:itca_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:last), '1900-01-01')) is null and 
                    src:last is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lrev), '0'), 38, 10) is null and 
                    src:lrev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltad_1), '0'), 38, 10) is null and 
                    src:ltad_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltad_2), '0'), 38, 10) is null and 
                    src:ltad_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltad_3), '0'), 38, 10) is null and 
                    src:ltad_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltar_1), '0'), 38, 10) is null and 
                    src:ltar_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltar_2), '0'), 38, 10) is null and 
                    src:ltar_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltar_3), '0'), 38, 10) is null and 
                    src:ltar_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_1), '0'), 38, 10) is null and 
                    src:ltdd_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_2), '0'), 38, 10) is null and 
                    src:ltdd_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_3), '0'), 38, 10) is null and 
                    src:ltdd_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdr_1), '0'), 38, 10) is null and 
                    src:ltdr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdr_2), '0'), 38, 10) is null and 
                    src:ltdr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdr_3), '0'), 38, 10) is null and 
                    src:ltdr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:meth_ref_compnr), '0'), 38, 10) is null and 
                    src:meth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_1), '0'), 38, 10) is null and 
                    src:ocst_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_2), '0'), 38, 10) is null and 
                    src:ocst_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_3), '0'), 38, 10) is null and 
                    src:ocst_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is null and 
                    src:prop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_1), '0'), 38, 10) is null and 
                    src:rcst_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_2), '0'), 38, 10) is null and 
                    src:rcst_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_3), '0'), 38, 10) is null and 
                    src:rcst_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpr_1), '0'), 38, 10) is null and 
                    src:rdpr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpr_2), '0'), 38, 10) is null and 
                    src:rdpr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpr_3), '0'), 38, 10) is null and 
                    src:rdpr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_1), '0'), 38, 10) is null and 
                    src:s179_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_2), '0'), 38, 10) is null and 
                    src:s179_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_3), '0'), 38, 10) is null and 
                    src:s179_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdpn), '0'), 38, 10) is null and 
                    src:sdpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shft), '0'), 38, 10) is null and 
                    src:shft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srac), '0'), 38, 10) is null and 
                    src:srac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmth), '0'), 38, 10) is null and 
                    src:tmth is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unit), '0'), 38, 10) is null and 
                    src:unit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_1), '0'), 38, 10) is null and 
                    src:ytdd_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_2), '0'), 38, 10) is null and 
                    src:ytdd_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_3), '0'), 38, 10) is null and 
                    src:ytdd_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_1), '0'), 38, 10) is null and 
                    src:ytdr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_2), '0'), 38, 10) is null and 
                    src:ytdr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_3), '0'), 38, 10) is null and 
                    src:ytdr_3 is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)