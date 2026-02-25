CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM200_ERROR AS SELECT src, 'LN_TFFAM200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aexm_ref_compnr), '0'), 38, 10) is null and 
                    src:aexm_ref_compnr is not null) THEN
                    'aexm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:aexm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is null and 
                    src:agrp_ref_compnr is not null) THEN
                    'agrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:agrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_ref_compnr), '0'), 38, 10) is null and 
                    src:amth_ref_compnr is not null) THEN
                    'amth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:amth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbm_ref_compnr), '0'), 38, 10) is null and 
                    src:anbm_ref_compnr is not null) THEN
                    'anbm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:anbm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aslf), '0'), 38, 10) is null and 
                    src:aslf is not null) THEN
                    'aslf contains a non-numeric value : \'' || AS_VARCHAR(src:aslf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bslv), '0'), 38, 10) is null and 
                    src:bslv is not null) THEN
                    'bslv contains a non-numeric value : \'' || AS_VARCHAR(src:bslv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmth_ref_compnr), '0'), 38, 10) is null and 
                    src:cmth_ref_compnr is not null) THEN
                    'cmth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cthr), '0'), 38, 10) is null and 
                    src:cthr is not null) THEN
                    'cthr contains a non-numeric value : \'' || AS_VARCHAR(src:cthr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dscr), '0'), 38, 10) is null and 
                    src:dscr is not null) THEN
                    'dscr contains a non-numeric value : \'' || AS_VARCHAR(src:dscr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fadr), '0'), 38, 10) is null and 
                    src:fadr is not null) THEN
                    'fadr contains a non-numeric value : \'' || AS_VARCHAR(src:fadr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fmth_ref_compnr), '0'), 38, 10) is null and 
                    src:fmth_ref_compnr is not null) THEN
                    'fmth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fmth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcd), '0'), 38, 10) is null and 
                    src:itcd is not null) THEN
                    'itcd contains a non-numeric value : \'' || AS_VARCHAR(src:itcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) THEN
                    'life contains a non-numeric value : \'' || AS_VARCHAR(src:life) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmth_ref_compnr), '0'), 38, 10) is null and 
                    src:lmth_ref_compnr is not null) THEN
                    'lmth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lmth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmth_ref_compnr), '0'), 38, 10) is null and 
                    src:mmth_ref_compnr is not null) THEN
                    'mmth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mmth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mskc), '0'), 38, 10) is null and 
                    src:mskc is not null) THEN
                    'mskc contains a non-numeric value : \'' || AS_VARCHAR(src:mskc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mskc_ref_compnr), '0'), 38, 10) is null and 
                    src:mskc_ref_compnr is not null) THEN
                    'mskc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mskc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:omth_ref_compnr), '0'), 38, 10) is null and 
                    src:omth_ref_compnr is not null) THEN
                    'omth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:omth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is null and 
                    src:prop_ref_compnr is not null) THEN
                    'prop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smth_ref_compnr), '0'), 38, 10) is null and 
                    src:smth_ref_compnr is not null) THEN
                    'smth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:smth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmth_ref_compnr), '0'), 38, 10) is null and 
                    src:tmth_ref_compnr is not null) THEN
                    'tmth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tmth_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umth_ref_compnr), '0'), 38, 10) is null and 
                    src:umth_ref_compnr is not null) THEN
                    'umth_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:umth_ref_compnr) || '\'' WHEN 
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
                                    
                src:ctgy,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aexm_ref_compnr), '0'), 38, 10) is null and 
                    src:aexm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is null and 
                    src:agrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_ref_compnr), '0'), 38, 10) is null and 
                    src:amth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbm_ref_compnr), '0'), 38, 10) is null and 
                    src:anbm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aslf), '0'), 38, 10) is null and 
                    src:aslf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bslv), '0'), 38, 10) is null and 
                    src:bslv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmth_ref_compnr), '0'), 38, 10) is null and 
                    src:cmth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cthr), '0'), 38, 10) is null and 
                    src:cthr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dscr), '0'), 38, 10) is null and 
                    src:dscr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fadr), '0'), 38, 10) is null and 
                    src:fadr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fmth_ref_compnr), '0'), 38, 10) is null and 
                    src:fmth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcd), '0'), 38, 10) is null and 
                    src:itcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmth_ref_compnr), '0'), 38, 10) is null and 
                    src:lmth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmth_ref_compnr), '0'), 38, 10) is null and 
                    src:mmth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mskc), '0'), 38, 10) is null and 
                    src:mskc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mskc_ref_compnr), '0'), 38, 10) is null and 
                    src:mskc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:omth_ref_compnr), '0'), 38, 10) is null and 
                    src:omth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is null and 
                    src:prop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smth_ref_compnr), '0'), 38, 10) is null and 
                    src:smth_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmth_ref_compnr), '0'), 38, 10) is null and 
                    src:tmth_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umth_ref_compnr), '0'), 38, 10) is null and 
                    src:umth_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)