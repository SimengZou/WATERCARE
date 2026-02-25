CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP251_ERROR AS SELECT src, 'LN_TFACP251' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) THEN
                    'amth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) THEN
                    'amth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) THEN
                    'amth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) THEN
                    'aprp contains a non-numeric value : \'' || AS_VARCHAR(src:aprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apry), '0'), 38, 10) is null and 
                    src:apry is not null) THEN
                    'apry contains a non-numeric value : \'' || AS_VARCHAR(src:apry) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buex), '0'), 38, 10) is null and 
                    src:buex is not null) THEN
                    'buex contains a non-numeric value : \'' || AS_VARCHAR(src:buex) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:data), '1900-01-01')) is null and 
                    src:data is not null) THEN
                    'data contains a non-timestamp value : \'' || AS_VARCHAR(src:data) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbmo), '0'), 38, 10) is null and 
                    src:dbmo is not null) THEN
                    'dbmo contains a non-numeric value : \'' || AS_VARCHAR(src:dbmo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:finl), '0'), 38, 10) is null and 
                    src:finl is not null) THEN
                    'finl contains a non-numeric value : \'' || AS_VARCHAR(src:finl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) THEN
                    'iamt contains a non-numeric value : \'' || AS_VARCHAR(src:iamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icom), '0'), 38, 10) is null and 
                    src:icom is not null) THEN
                    'icom contains a non-numeric value : \'' || AS_VARCHAR(src:icom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iqan), '0'), 38, 10) is null and 
                    src:iqan is not null) THEN
                    'iqan contains a non-numeric value : \'' || AS_VARCHAR(src:iqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) THEN
                    'loco contains a non-numeric value : \'' || AS_VARCHAR(src:loco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) THEN
                    'otyp contains a non-numeric value : \'' || AS_VARCHAR(src:otyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdfd), '0'), 38, 10) is null and 
                    src:pdfd is not null) THEN
                    'pdfd contains a non-numeric value : \'' || AS_VARCHAR(src:pdfd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdif), '0'), 38, 10) is null and 
                    src:pdif is not null) THEN
                    'pdif contains a non-numeric value : \'' || AS_VARCHAR(src:pdif) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr), '0'), 38, 10) is null and 
                    src:shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr is not null) THEN
                    'shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                src:loco,
                src:pono,
                src:icom,
                src:idoc,
                src:ityp,
                src:rseq,
                src:otyp,
                src:rcno,
                src:shpm,
                src:orno,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP251 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apry), '0'), 38, 10) is null and 
                    src:apry is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buex), '0'), 38, 10) is null and 
                    src:buex is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:data), '1900-01-01')) is null and 
                    src:data is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbmo), '0'), 38, 10) is null and 
                    src:dbmo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:finl), '0'), 38, 10) is null and 
                    src:finl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icom), '0'), 38, 10) is null and 
                    src:icom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iqan), '0'), 38, 10) is null and 
                    src:iqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdfd), '0'), 38, 10) is null and 
                    src:pdfd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdif), '0'), 38, 10) is null and 
                    src:pdif is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr), '0'), 38, 10) is null and 
                    src:shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)