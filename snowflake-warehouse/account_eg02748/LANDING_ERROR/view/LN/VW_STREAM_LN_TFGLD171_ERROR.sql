CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD171_ERROR AS SELECT src, 'LN_TFGLD171' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acty), '0'), 38, 10) is null and 
                    src:acty is not null) THEN
                    'acty contains a non-numeric value : \'' || AS_VARCHAR(src:acty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mapp), '0'), 38, 10) is null and 
                    src:mapp is not null) THEN
                    'mapp contains a non-numeric value : \'' || AS_VARCHAR(src:mapp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) THEN
                    'subl contains a non-numeric value : \'' || AS_VARCHAR(src:subl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxo_vers_ref_compnr), '0'), 38, 10) is null and 
                    src:taxo_vers_ref_compnr is not null) THEN
                    'taxo_vers_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:taxo_vers_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unas), '0'), 38, 10) is null and 
                    src:unas is not null) THEN
                    'unas contains a non-numeric value : \'' || AS_VARCHAR(src:unas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vers), '0'), 38, 10) is null and 
                    src:vers is not null) THEN
                    'vers contains a non-numeric value : \'' || AS_VARCHAR(src:vers) || '\'' WHEN 
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
                src:taxo,
                src:acid,
                src:vers  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD171 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acty), '0'), 38, 10) is null and 
                    src:acty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mapp), '0'), 38, 10) is null and 
                    src:mapp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxo_vers_ref_compnr), '0'), 38, 10) is null and 
                    src:taxo_vers_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unas), '0'), 38, 10) is null and 
                    src:unas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vers), '0'), 38, 10) is null and 
                    src:vers is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)