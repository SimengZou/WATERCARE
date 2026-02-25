CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP245_ERROR AS SELECT src, 'LN_TFACP245' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cval), '0'), 38, 10) is null and 
                    src:cval is not null) THEN
                    'cval contains a non-numeric value : \'' || AS_VARCHAR(src:cval) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvlc_ref_compnr), '0'), 38, 10) is null and 
                    src:cvlc_ref_compnr is not null) THEN
                    'cvlc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvlc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcpo), '0'), 38, 10) is null and 
                    src:fcpo is not null) THEN
                    'fcpo contains a non-numeric value : \'' || AS_VARCHAR(src:fcpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmat), '0'), 38, 10) is null and 
                    src:lmat is not null) THEN
                    'lmat contains a non-numeric value : \'' || AS_VARCHAR(src:lmat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) THEN
                    'loco contains a non-numeric value : \'' || AS_VARCHAR(src:loco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco_otyp_orno_pono_sqnb_ref_compnr), '0'), 38, 10) is null and 
                    src:loco_otyp_orno_pono_sqnb_ref_compnr is not null) THEN
                    'loco_otyp_orno_pono_sqnb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:loco_otyp_orno_pono_sqnb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maam), '0'), 38, 10) is null and 
                    src:maam is not null) THEN
                    'maam contains a non-numeric value : \'' || AS_VARCHAR(src:maam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maqu), '0'), 38, 10) is null and 
                    src:maqu is not null) THEN
                    'maqu contains a non-numeric value : \'' || AS_VARCHAR(src:maqu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mqpu), '0'), 38, 10) is null and 
                    src:mqpu is not null) THEN
                    'mqpu contains a non-numeric value : \'' || AS_VARCHAR(src:mqpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtch), '0'), 38, 10) is null and 
                    src:mtch is not null) THEN
                    'mtch contains a non-numeric value : \'' || AS_VARCHAR(src:mtch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:omti), '0'), 38, 10) is null and 
                    src:omti is not null) THEN
                    'omti contains a non-numeric value : \'' || AS_VARCHAR(src:omti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) THEN
                    'otyp contains a non-numeric value : \'' || AS_VARCHAR(src:otyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) THEN
                    'rdat contains a non-timestamp value : \'' || AS_VARCHAR(src:rdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqty), '0'), 38, 10) is null and 
                    src:rqty is not null) THEN
                    'rqty contains a non-numeric value : \'' || AS_VARCHAR(src:rqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbdt), '0'), 38, 10) is null and 
                    src:sbdt is not null) THEN
                    'sbdt contains a non-numeric value : \'' || AS_VARCHAR(src:sbdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbl), '0'), 38, 10) is null and 
                    src:sfbl is not null) THEN
                    'sfbl contains a non-numeric value : \'' || AS_VARCHAR(src:sfbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toma), '0'), 38, 10) is null and 
                    src:toma is not null) THEN
                    'toma contains a non-numeric value : \'' || AS_VARCHAR(src:toma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trqn), '0'), 38, 10) is null and 
                    src:trqn is not null) THEN
                    'trqn contains a non-numeric value : \'' || AS_VARCHAR(src:trqn) || '\'' WHEN 
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
                                    
                src:shpm,
                src:sqnb,
                src:rcno,
                src:pono,
                src:rseq,
                src:loco,
                src:otyp,
                src:compnr,
                src:orno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP245 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cval), '0'), 38, 10) is null and 
                    src:cval is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvlc_ref_compnr), '0'), 38, 10) is null and 
                    src:cvlc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcpo), '0'), 38, 10) is null and 
                    src:fcpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmat), '0'), 38, 10) is null and 
                    src:lmat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco_otyp_orno_pono_sqnb_ref_compnr), '0'), 38, 10) is null and 
                    src:loco_otyp_orno_pono_sqnb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maam), '0'), 38, 10) is null and 
                    src:maam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maqu), '0'), 38, 10) is null and 
                    src:maqu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mqpu), '0'), 38, 10) is null and 
                    src:mqpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtch), '0'), 38, 10) is null and 
                    src:mtch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:omti), '0'), 38, 10) is null and 
                    src:omti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdat), '1900-01-01')) is null and 
                    src:rdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqty), '0'), 38, 10) is null and 
                    src:rqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbdt), '0'), 38, 10) is null and 
                    src:sbdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbl), '0'), 38, 10) is null and 
                    src:sfbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:toma), '0'), 38, 10) is null and 
                    src:toma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trqn), '0'), 38, 10) is null and 
                    src:trqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uppr), '0'), 38, 10) is null and 
                    src:uppr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)