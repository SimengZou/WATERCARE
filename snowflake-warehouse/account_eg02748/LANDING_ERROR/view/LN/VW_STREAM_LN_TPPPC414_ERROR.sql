CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC414_ERROR AS SELECT src, 'LN_TPPPC414' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_1), '0'), 38, 10) is null and 
                    src:amac_1 is not null) THEN
                    'amac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_2), '0'), 38, 10) is null and 
                    src:amac_2 is not null) THEN
                    'amac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_3), '0'), 38, 10) is null and 
                    src:amac_3 is not null) THEN
                    'amac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_4), '0'), 38, 10) is null and 
                    src:amac_4 is not null) THEN
                    'amac_4 contains a non-numeric value : \'' || AS_VARCHAR(src:amac_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_1), '0'), 38, 10) is null and 
                    src:amap_1 is not null) THEN
                    'amap_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amap_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_2), '0'), 38, 10) is null and 
                    src:amap_2 is not null) THEN
                    'amap_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amap_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_3), '0'), 38, 10) is null and 
                    src:amap_3 is not null) THEN
                    'amap_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amap_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_4), '0'), 38, 10) is null and 
                    src:amap_4 is not null) THEN
                    'amap_4 contains a non-numeric value : \'' || AS_VARCHAR(src:amap_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_1), '0'), 38, 10) is null and 
                    src:ambg_1 is not null) THEN
                    'ambg_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ambg_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_2), '0'), 38, 10) is null and 
                    src:ambg_2 is not null) THEN
                    'ambg_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ambg_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_3), '0'), 38, 10) is null and 
                    src:ambg_3 is not null) THEN
                    'ambg_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ambg_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_4), '0'), 38, 10) is null and 
                    src:ambg_4 is not null) THEN
                    'ambg_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ambg_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_1), '0'), 38, 10) is null and 
                    src:amep_1 is not null) THEN
                    'amep_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amep_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_2), '0'), 38, 10) is null and 
                    src:amep_2 is not null) THEN
                    'amep_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amep_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_3), '0'), 38, 10) is null and 
                    src:amep_3 is not null) THEN
                    'amep_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amep_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_4), '0'), 38, 10) is null and 
                    src:amep_4 is not null) THEN
                    'amep_4 contains a non-numeric value : \'' || AS_VARCHAR(src:amep_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_1), '0'), 38, 10) is null and 
                    src:amex_1 is not null) THEN
                    'amex_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amex_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_2), '0'), 38, 10) is null and 
                    src:amex_2 is not null) THEN
                    'amex_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amex_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_3), '0'), 38, 10) is null and 
                    src:amex_3 is not null) THEN
                    'amex_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amex_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_4), '0'), 38, 10) is null and 
                    src:amex_4 is not null) THEN
                    'amex_4 contains a non-numeric value : \'' || AS_VARCHAR(src:amex_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_1), '0'), 38, 10) is null and 
                    src:ampe_1 is not null) THEN
                    'ampe_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ampe_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_2), '0'), 38, 10) is null and 
                    src:ampe_2 is not null) THEN
                    'ampe_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ampe_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_3), '0'), 38, 10) is null and 
                    src:ampe_3 is not null) THEN
                    'ampe_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ampe_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_4), '0'), 38, 10) is null and 
                    src:ampe_4 is not null) THEN
                    'ampe_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ampe_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_1), '0'), 38, 10) is null and 
                    src:ampm_1 is not null) THEN
                    'ampm_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ampm_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_2), '0'), 38, 10) is null and 
                    src:ampm_2 is not null) THEN
                    'ampm_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ampm_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_3), '0'), 38, 10) is null and 
                    src:ampm_3 is not null) THEN
                    'ampm_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ampm_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_4), '0'), 38, 10) is null and 
                    src:ampm_4 is not null) THEN
                    'ampm_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ampm_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_1), '0'), 38, 10) is null and 
                    src:ampp_1 is not null) THEN
                    'ampp_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ampp_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_2), '0'), 38, 10) is null and 
                    src:ampp_2 is not null) THEN
                    'ampp_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ampp_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_3), '0'), 38, 10) is null and 
                    src:ampp_3 is not null) THEN
                    'ampp_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ampp_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_4), '0'), 38, 10) is null and 
                    src:ampp_4 is not null) THEN
                    'ampp_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ampp_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quac), '0'), 38, 10) is null and 
                    src:quac is not null) THEN
                    'quac contains a non-numeric value : \'' || AS_VARCHAR(src:quac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quap), '0'), 38, 10) is null and 
                    src:quap is not null) THEN
                    'quap contains a non-numeric value : \'' || AS_VARCHAR(src:quap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qubg), '0'), 38, 10) is null and 
                    src:qubg is not null) THEN
                    'qubg contains a non-numeric value : \'' || AS_VARCHAR(src:qubg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quep), '0'), 38, 10) is null and 
                    src:quep is not null) THEN
                    'quep contains a non-numeric value : \'' || AS_VARCHAR(src:quep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quex), '0'), 38, 10) is null and 
                    src:quex is not null) THEN
                    'quex contains a non-numeric value : \'' || AS_VARCHAR(src:quex) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qupe), '0'), 38, 10) is null and 
                    src:qupe is not null) THEN
                    'qupe contains a non-numeric value : \'' || AS_VARCHAR(src:qupe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qupm), '0'), 38, 10) is null and 
                    src:qupm is not null) THEN
                    'qupm contains a non-numeric value : \'' || AS_VARCHAR(src:qupm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qupp), '0'), 38, 10) is null and 
                    src:qupp is not null) THEN
                    'qupp contains a non-numeric value : \'' || AS_VARCHAR(src:qupp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
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
                                    
                src:cprj,
                src:ccic,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC414 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_1), '0'), 38, 10) is null and 
                    src:amac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_2), '0'), 38, 10) is null and 
                    src:amac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_3), '0'), 38, 10) is null and 
                    src:amac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amac_4), '0'), 38, 10) is null and 
                    src:amac_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_1), '0'), 38, 10) is null and 
                    src:amap_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_2), '0'), 38, 10) is null and 
                    src:amap_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_3), '0'), 38, 10) is null and 
                    src:amap_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amap_4), '0'), 38, 10) is null and 
                    src:amap_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_1), '0'), 38, 10) is null and 
                    src:ambg_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_2), '0'), 38, 10) is null and 
                    src:ambg_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_3), '0'), 38, 10) is null and 
                    src:ambg_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ambg_4), '0'), 38, 10) is null and 
                    src:ambg_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_1), '0'), 38, 10) is null and 
                    src:amep_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_2), '0'), 38, 10) is null and 
                    src:amep_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_3), '0'), 38, 10) is null and 
                    src:amep_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amep_4), '0'), 38, 10) is null and 
                    src:amep_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_1), '0'), 38, 10) is null and 
                    src:amex_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_2), '0'), 38, 10) is null and 
                    src:amex_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_3), '0'), 38, 10) is null and 
                    src:amex_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amex_4), '0'), 38, 10) is null and 
                    src:amex_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_1), '0'), 38, 10) is null and 
                    src:ampe_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_2), '0'), 38, 10) is null and 
                    src:ampe_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_3), '0'), 38, 10) is null and 
                    src:ampe_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampe_4), '0'), 38, 10) is null and 
                    src:ampe_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_1), '0'), 38, 10) is null and 
                    src:ampm_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_2), '0'), 38, 10) is null and 
                    src:ampm_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_3), '0'), 38, 10) is null and 
                    src:ampm_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampm_4), '0'), 38, 10) is null and 
                    src:ampm_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_1), '0'), 38, 10) is null and 
                    src:ampp_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_2), '0'), 38, 10) is null and 
                    src:ampp_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_3), '0'), 38, 10) is null and 
                    src:ampp_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ampp_4), '0'), 38, 10) is null and 
                    src:ampp_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quac), '0'), 38, 10) is null and 
                    src:quac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quap), '0'), 38, 10) is null and 
                    src:quap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qubg), '0'), 38, 10) is null and 
                    src:qubg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quep), '0'), 38, 10) is null and 
                    src:quep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quex), '0'), 38, 10) is null and 
                    src:quex is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qupe), '0'), 38, 10) is null and 
                    src:qupe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qupm), '0'), 38, 10) is null and 
                    src:qupm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qupp), '0'), 38, 10) is null and 
                    src:qupp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)