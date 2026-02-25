CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM800_ERROR AS SELECT src, 'LN_TFFAM800' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) THEN
                    'amnt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) THEN
                    'amnt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) THEN
                    'amnt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_book_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_book_ref_compnr is not null) THEN
                    'anbr_aext_book_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:anbr_aext_book_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_ref_compnr is not null) THEN
                    'anbr_aext_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:anbr_aext_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atty), '0'), 38, 10) is null and 
                    src:atty is not null) THEN
                    'atty contains a non-numeric value : \'' || AS_VARCHAR(src:atty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is null and 
                    src:book_ref_compnr is not null) THEN
                    'book_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:book_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) THEN
                    'docn contains a non-numeric value : \'' || AS_VARCHAR(src:docn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:line), '0'), 38, 10) is null and 
                    src:line is not null) THEN
                    'line contains a non-numeric value : \'' || AS_VARCHAR(src:line) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) THEN
                    'post contains a non-numeric value : \'' || AS_VARCHAR(src:post) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) THEN
                    'prod contains a non-numeric value : \'' || AS_VARCHAR(src:prod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rort), '0'), 38, 10) is null and 
                    src:rort is not null) THEN
                    'rort contains a non-numeric value : \'' || AS_VARCHAR(src:rort) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tdat), '1900-01-01')) is null and 
                    src:tdat is not null) THEN
                    'tdat contains a non-timestamp value : \'' || AS_VARCHAR(src:tdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) THEN
                    'tkey contains a non-numeric value : \'' || AS_VARCHAR(src:tkey) || '\'' WHEN 
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
                                    
                src:compnr,
                src:tkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM800 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is null and 
                    src:amnt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is null and 
                    src:amnt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is null and 
                    src:amnt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_book_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_book_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atty), '0'), 38, 10) is null and 
                    src:atty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is null and 
                    src:book_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:line), '0'), 38, 10) is null and 
                    src:line is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rort), '0'), 38, 10) is null and 
                    src:rort is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tdat), '1900-01-01')) is null and 
                    src:tdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tkey), '0'), 38, 10) is null and 
                    src:tkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)