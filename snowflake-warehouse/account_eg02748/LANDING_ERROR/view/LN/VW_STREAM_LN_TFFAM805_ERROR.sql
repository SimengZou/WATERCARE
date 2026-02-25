CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM805_ERROR AS SELECT src, 'LN_TFFAM805' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_1), '0'), 38, 10) is null and 
                    src:depr_1 is not null) THEN
                    'depr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_2), '0'), 38, 10) is null and 
                    src:depr_2 is not null) THEN
                    'depr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_3), '0'), 38, 10) is null and 
                    src:depr_3 is not null) THEN
                    'depr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkey), '0'), 38, 10) is null and 
                    src:lkey is not null) THEN
                    'lkey contains a non-numeric value : \'' || AS_VARCHAR(src:lkey) || '\'' WHEN 
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
                                    
                src:book,
                src:lkey,
                src:anbr,
                src:compnr,
                src:tkey,
                src:bpid,
                src:aext  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM805 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_1), '0'), 38, 10) is null and 
                    src:cost_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_2), '0'), 38, 10) is null and 
                    src:cost_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost_3), '0'), 38, 10) is null and 
                    src:cost_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_1), '0'), 38, 10) is null and 
                    src:depr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_2), '0'), 38, 10) is null and 
                    src:depr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_3), '0'), 38, 10) is null and 
                    src:depr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkey), '0'), 38, 10) is null and 
                    src:lkey is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey_ref_compnr), '0'), 38, 10) is null and 
                    src:tkey_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)