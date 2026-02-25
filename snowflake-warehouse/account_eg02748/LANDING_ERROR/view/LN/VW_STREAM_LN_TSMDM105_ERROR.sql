CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSMDM105_ERROR AS SELECT src, 'LN_TSMDM105' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:artp), '0'), 38, 10) is null and 
                    src:artp is not null) THEN
                    'artp contains a non-numeric value : \'' || AS_VARCHAR(src:artp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccha), '0'), 38, 10) is null and 
                    src:ccha is not null) THEN
                    'ccha contains a non-numeric value : \'' || AS_VARCHAR(src:ccha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) THEN
                    'clrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmsa_ref_compnr), '0'), 38, 10) is null and 
                    src:cmsa_ref_compnr is not null) THEN
                    'cmsa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmsa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dist), '0'), 38, 10) is null and 
                    src:dist is not null) THEN
                    'dist contains a non-numeric value : \'' || AS_VARCHAR(src:dist) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trti), '0'), 38, 10) is null and 
                    src:trti is not null) THEN
                    'trti contains a non-numeric value : \'' || AS_VARCHAR(src:trti) || '\'' WHEN 
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
                src:csar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSMDM105 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:artp), '0'), 38, 10) is null and 
                    src:artp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccha), '0'), 38, 10) is null and 
                    src:ccha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmsa_ref_compnr), '0'), 38, 10) is null and 
                    src:cmsa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dist), '0'), 38, 10) is null and 
                    src:dist is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trti), '0'), 38, 10) is null and 
                    src:trti is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)