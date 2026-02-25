CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM810_ERROR AS SELECT src, 'LN_TFFAM810' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is null and 
                    src:acmc_1 is not null) THEN
                    'acmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is null and 
                    src:acmc_2 is not null) THEN
                    'acmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is null and 
                    src:acmc_3 is not null) THEN
                    'acmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmt), '0'), 38, 10) is null and 
                    src:acmt is not null) THEN
                    'acmt contains a non-numeric value : \'' || AS_VARCHAR(src:acmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is null and 
                    src:agrp_ref_compnr is not null) THEN
                    'agrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:agrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) THEN
                    'amnt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) THEN
                    'amnt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) THEN
                    'amnt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt), '0'), 38, 10) is null and 
                    src:amtt is not null) THEN
                    'amtt contains a non-numeric value : \'' || AS_VARCHAR(src:amtt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_ref_compnr is not null) THEN
                    'ctgy_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctgy_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_sctg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_sctg_ref_compnr is not null) THEN
                    'ctgy_sctg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctgy_sctg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jobn), '0'), 38, 10) is null and 
                    src:jobn is not null) THEN
                    'jobn contains a non-numeric value : \'' || AS_VARCHAR(src:jobn) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_1), '0'), 38, 10) is null and 
                    src:rcst_1 is not null) THEN
                    'rcst_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rcst_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_2), '0'), 38, 10) is null and 
                    src:rcst_2 is not null) THEN
                    'rcst_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rcst_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_3), '0'), 38, 10) is null and 
                    src:rcst_3 is not null) THEN
                    'rcst_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rcst_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) THEN
                    'reas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) THEN
                    'tkey contains a non-numeric value : \'' || AS_VARCHAR(src:tkey) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey_ref_compnr), '0'), 38, 10) is null and 
                    src:tkey_ref_compnr is not null) THEN
                    'tkey_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tkey_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trid), '0'), 38, 10) is null and 
                    src:trid is not null) THEN
                    'trid contains a non-numeric value : \'' || AS_VARCHAR(src:trid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xcom), '0'), 38, 10) is null and 
                    src:xcom is not null) THEN
                    'xcom contains a non-numeric value : \'' || AS_VARCHAR(src:xcom) || '\'' WHEN 
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
                src:tkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM810 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is null and 
                    src:acmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is null and 
                    src:acmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is null and 
                    src:acmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmt), '0'), 38, 10) is null and 
                    src:acmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is null and 
                    src:agrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt), '0'), 38, 10) is null and 
                    src:amtt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_sctg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_sctg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jobn), '0'), 38, 10) is null and 
                    src:jobn is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_1), '0'), 38, 10) is null and 
                    src:rcst_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_2), '0'), 38, 10) is null and 
                    src:rcst_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_3), '0'), 38, 10) is null and 
                    src:rcst_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey_ref_compnr), '0'), 38, 10) is null and 
                    src:tkey_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trid), '0'), 38, 10) is null and 
                    src:trid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xcom), '0'), 38, 10) is null and 
                    src:xcom is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)