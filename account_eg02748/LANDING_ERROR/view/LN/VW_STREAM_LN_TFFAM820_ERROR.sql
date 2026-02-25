CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM820_ERROR AS SELECT src, 'LN_TFFAM820' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is null and 
                    src:acmc_1 is not null) THEN
                    'acmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is null and 
                    src:acmc_2 is not null) THEN
                    'acmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is null and 
                    src:acmc_3 is not null) THEN
                    'acmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmz), '0'), 38, 10) is null and 
                    src:acmz is not null) THEN
                    'acmz contains a non-numeric value : \'' || AS_VARCHAR(src:acmz) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apam_1), '0'), 38, 10) is null and 
                    src:apam_1 is not null) THEN
                    'apam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:apam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apam_2), '0'), 38, 10) is null and 
                    src:apam_2 is not null) THEN
                    'apam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:apam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apam_3), '0'), 38, 10) is null and 
                    src:apam_3 is not null) THEN
                    'apam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:apam_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apaz), '0'), 38, 10) is null and 
                    src:apaz is not null) THEN
                    'apaz contains a non-numeric value : \'' || AS_VARCHAR(src:apaz) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_1), '0'), 38, 10) is null and 
                    src:capi_1 is not null) THEN
                    'capi_1 contains a non-numeric value : \'' || AS_VARCHAR(src:capi_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_2), '0'), 38, 10) is null and 
                    src:capi_2 is not null) THEN
                    'capi_2 contains a non-numeric value : \'' || AS_VARCHAR(src:capi_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_3), '0'), 38, 10) is null and 
                    src:capi_3 is not null) THEN
                    'capi_3 contains a non-numeric value : \'' || AS_VARCHAR(src:capi_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capz), '0'), 38, 10) is null and 
                    src:capz is not null) THEN
                    'capz contains a non-numeric value : \'' || AS_VARCHAR(src:capz) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cosz), '0'), 38, 10) is null and 
                    src:cosz is not null) THEN
                    'cosz contains a non-numeric value : \'' || AS_VARCHAR(src:cosz) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_1), '0'), 38, 10) is null and 
                    src:itca_1 is not null) THEN
                    'itca_1 contains a non-numeric value : \'' || AS_VARCHAR(src:itca_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_2), '0'), 38, 10) is null and 
                    src:itca_2 is not null) THEN
                    'itca_2 contains a non-numeric value : \'' || AS_VARCHAR(src:itca_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_3), '0'), 38, 10) is null and 
                    src:itca_3 is not null) THEN
                    'itca_3 contains a non-numeric value : \'' || AS_VARCHAR(src:itca_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcz), '0'), 38, 10) is null and 
                    src:itcz is not null) THEN
                    'itcz contains a non-numeric value : \'' || AS_VARCHAR(src:itcz) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) THEN
                    'life contains a non-numeric value : \'' || AS_VARCHAR(src:life) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lifz), '0'), 38, 10) is null and 
                    src:lifz is not null) THEN
                    'lifz contains a non-numeric value : \'' || AS_VARCHAR(src:lifz) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_1), '0'), 38, 10) is null and 
                    src:ltdd_1 is not null) THEN
                    'ltdd_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_2), '0'), 38, 10) is null and 
                    src:ltdd_2 is not null) THEN
                    'ltdd_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_3), '0'), 38, 10) is null and 
                    src:ltdd_3 is not null) THEN
                    'ltdd_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdz), '0'), 38, 10) is null and 
                    src:ltdz is not null) THEN
                    'ltdz contains a non-numeric value : \'' || AS_VARCHAR(src:ltdz) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) THEN
                    'reas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) THEN
                    'revl contains a non-numeric value : \'' || AS_VARCHAR(src:revl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_1), '0'), 38, 10) is null and 
                    src:s179_1 is not null) THEN
                    's179_1 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_2), '0'), 38, 10) is null and 
                    src:s179_2 is not null) THEN
                    's179_2 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_3), '0'), 38, 10) is null and 
                    src:s179_3 is not null) THEN
                    's179_3 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s17z), '0'), 38, 10) is null and 
                    src:s17z is not null) THEN
                    's17z contains a non-numeric value : \'' || AS_VARCHAR(src:s17z) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) THEN
                    'salv_1 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) THEN
                    'salv_2 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) THEN
                    'salv_3 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salz), '0'), 38, 10) is null and 
                    src:salz is not null) THEN
                    'salz contains a non-numeric value : \'' || AS_VARCHAR(src:salz) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traf), '0'), 38, 10) is null and 
                    src:traf is not null) THEN
                    'traf contains a non-numeric value : \'' || AS_VARCHAR(src:traf) || '\'' WHEN 
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
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM820 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is null and 
                    src:acmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is null and 
                    src:acmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is null and 
                    src:acmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmz), '0'), 38, 10) is null and 
                    src:acmz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apam_1), '0'), 38, 10) is null and 
                    src:apam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apam_2), '0'), 38, 10) is null and 
                    src:apam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apam_3), '0'), 38, 10) is null and 
                    src:apam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apaz), '0'), 38, 10) is null and 
                    src:apaz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_1), '0'), 38, 10) is null and 
                    src:capi_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_2), '0'), 38, 10) is null and 
                    src:capi_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi_3), '0'), 38, 10) is null and 
                    src:capi_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capz), '0'), 38, 10) is null and 
                    src:capz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_1), '0'), 38, 10) is null and 
                    src:cost_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_2), '0'), 38, 10) is null and 
                    src:cost_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_3), '0'), 38, 10) is null and 
                    src:cost_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cosz), '0'), 38, 10) is null and 
                    src:cosz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_1), '0'), 38, 10) is null and 
                    src:itca_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_2), '0'), 38, 10) is null and 
                    src:itca_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca_3), '0'), 38, 10) is null and 
                    src:itca_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcz), '0'), 38, 10) is null and 
                    src:itcz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lifz), '0'), 38, 10) is null and 
                    src:lifz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_1), '0'), 38, 10) is null and 
                    src:ltdd_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_2), '0'), 38, 10) is null and 
                    src:ltdd_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd_3), '0'), 38, 10) is null and 
                    src:ltdd_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdz), '0'), 38, 10) is null and 
                    src:ltdz is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revl), '0'), 38, 10) is null and 
                    src:revl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_1), '0'), 38, 10) is null and 
                    src:s179_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_2), '0'), 38, 10) is null and 
                    src:s179_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_3), '0'), 38, 10) is null and 
                    src:s179_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s17z), '0'), 38, 10) is null and 
                    src:s17z is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salz), '0'), 38, 10) is null and 
                    src:salz is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey_ref_compnr), '0'), 38, 10) is null and 
                    src:tkey_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traf), '0'), 38, 10) is null and 
                    src:traf is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)