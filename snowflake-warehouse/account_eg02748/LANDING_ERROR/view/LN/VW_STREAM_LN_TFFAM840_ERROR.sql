CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM840_ERROR AS SELECT src, 'LN_TFFAM840' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adrt), '0'), 38, 10) is null and 
                    src:adrt is not null) THEN
                    'adrt contains a non-numeric value : \'' || AS_VARCHAR(src:adrt) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dqty), '0'), 38, 10) is null and 
                    src:dqty is not null) THEN
                    'dqty contains a non-numeric value : \'' || AS_VARCHAR(src:dqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jobn), '0'), 38, 10) is null and 
                    src:jobn is not null) THEN
                    'jobn contains a non-numeric value : \'' || AS_VARCHAR(src:jobn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) THEN
                    'life contains a non-numeric value : \'' || AS_VARCHAR(src:life) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc_1), '0'), 38, 10) is null and 
                    src:proc_1 is not null) THEN
                    'proc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:proc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc_2), '0'), 38, 10) is null and 
                    src:proc_2 is not null) THEN
                    'proc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:proc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc_3), '0'), 38, 10) is null and 
                    src:proc_3 is not null) THEN
                    'proc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:proc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) THEN
                    'prod contains a non-numeric value : \'' || AS_VARCHAR(src:prod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prot), '0'), 38, 10) is null and 
                    src:prot is not null) THEN
                    'prot contains a non-numeric value : \'' || AS_VARCHAR(src:prot) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) THEN
                    'salv_1 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) THEN
                    'salv_2 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) THEN
                    'salv_3 contains a non-numeric value : \'' || AS_VARCHAR(src:salv_3) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:type), '0'), 38, 10) is null and 
                    src:type is not null) THEN
                    'type contains a non-numeric value : \'' || AS_VARCHAR(src:type) || '\'' WHEN 
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
                                    
                src:tkey,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM840 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adrt), '0'), 38, 10) is null and 
                    src:adrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_1), '0'), 38, 10) is null and 
                    src:cost_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_2), '0'), 38, 10) is null and 
                    src:cost_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_3), '0'), 38, 10) is null and 
                    src:cost_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dqty), '0'), 38, 10) is null and 
                    src:dqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jobn), '0'), 38, 10) is null and 
                    src:jobn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc_1), '0'), 38, 10) is null and 
                    src:proc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc_2), '0'), 38, 10) is null and 
                    src:proc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc_3), '0'), 38, 10) is null and 
                    src:proc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prot), '0'), 38, 10) is null and 
                    src:prot is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey_ref_compnr), '0'), 38, 10) is null and 
                    src:tkey_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:type), '0'), 38, 10) is null and 
                    src:type is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)