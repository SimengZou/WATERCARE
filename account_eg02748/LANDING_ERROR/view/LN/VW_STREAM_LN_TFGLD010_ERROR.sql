CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD010_ERROR AS SELECT src, 'LN_TFGLD010' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atyp), '0'), 38, 10) is null and 
                    src:atyp is not null) THEN
                    'atyp contains a non-numeric value : \'' || AS_VARCHAR(src:atyp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bfdt), '1900-01-01')) is null and 
                    src:bfdt is not null) THEN
                    'bfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:bfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc), '0'), 38, 10) is null and 
                    src:bloc is not null) THEN
                    'bloc contains a non-numeric value : \'' || AS_VARCHAR(src:bloc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:budt), '1900-01-01')) is null and 
                    src:budt is not null) THEN
                    'budt contains a non-timestamp value : \'' || AS_VARCHAR(src:budt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp), '0'), 38, 10) is null and 
                    src:dtyp is not null) THEN
                    'dtyp contains a non-numeric value : \'' || AS_VARCHAR(src:dtyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp_pdix_ref_compnr), '0'), 38, 10) is null and 
                    src:dtyp_pdix_ref_compnr is not null) THEN
                    'dtyp_pdix_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dtyp_pdix_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:dtyp_ref_compnr is not null) THEN
                    'dtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan1_ref_compnr), '0'), 38, 10) is null and 
                    src:qan1_ref_compnr is not null) THEN
                    'qan1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qan1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan2_ref_compnr), '0'), 38, 10) is null and 
                    src:qan2_ref_compnr is not null) THEN
                    'qan2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qan2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) THEN
                    'subl contains a non-numeric value : \'' || AS_VARCHAR(src:subl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni1), '0'), 38, 10) is null and 
                    src:uni1 is not null) THEN
                    'uni1 contains a non-numeric value : \'' || AS_VARCHAR(src:uni1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni2), '0'), 38, 10) is null and 
                    src:uni2 is not null) THEN
                    'uni2 contains a non-numeric value : \'' || AS_VARCHAR(src:uni2) || '\'' WHEN 
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
                src:dtyp,
                src:dimx  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atyp), '0'), 38, 10) is null and 
                    src:atyp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bfdt), '1900-01-01')) is null and 
                    src:bfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc), '0'), 38, 10) is null and 
                    src:bloc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:budt), '1900-01-01')) is null and 
                    src:budt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp), '0'), 38, 10) is null and 
                    src:dtyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp_pdix_ref_compnr), '0'), 38, 10) is null and 
                    src:dtyp_pdix_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:dtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan1_ref_compnr), '0'), 38, 10) is null and 
                    src:qan1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan2_ref_compnr), '0'), 38, 10) is null and 
                    src:qan2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni1), '0'), 38, 10) is null and 
                    src:uni1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni2), '0'), 38, 10) is null and 
                    src:uni2 is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)