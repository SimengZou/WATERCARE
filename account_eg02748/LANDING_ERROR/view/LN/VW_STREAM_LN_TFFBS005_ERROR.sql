CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFBS005_ERROR AS SELECT src, 'LN_TFFBS005' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:budg_ref_compnr), '0'), 38, 10) is null and 
                    src:budg_ref_compnr is not null) THEN
                    'budg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:budg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbud_ref_compnr), '0'), 38, 10) is null and 
                    src:cbud_ref_compnr is not null) THEN
                    'cbud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbyr), '0'), 38, 10) is null and 
                    src:cbyr is not null) THEN
                    'cbyr contains a non-numeric value : \'' || AS_VARCHAR(src:cbyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbyr_cbud_ref_compnr), '0'), 38, 10) is null and 
                    src:cbyr_cbud_ref_compnr is not null) THEN
                    'cbyr_cbud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbyr_cbud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbud_ref_compnr), '0'), 38, 10) is null and 
                    src:dbud_ref_compnr is not null) THEN
                    'dbud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dbud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbyr), '0'), 38, 10) is null and 
                    src:dbyr is not null) THEN
                    'dbyr contains a non-numeric value : \'' || AS_VARCHAR(src:dbyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbyr_dbud_ref_compnr), '0'), 38, 10) is null and 
                    src:dbyr_dbud_ref_compnr is not null) THEN
                    'dbyr_dbud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dbyr_dbud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defi), '0'), 38, 10) is null and 
                    src:defi is not null) THEN
                    'defi contains a non-numeric value : \'' || AS_VARCHAR(src:defi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disb_ref_compnr), '0'), 38, 10) is null and 
                    src:disb_ref_compnr is not null) THEN
                    'disb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:disb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbud_ref_compnr), '0'), 38, 10) is null and 
                    src:pbud_ref_compnr is not null) THEN
                    'pbud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pbud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbyr), '0'), 38, 10) is null and 
                    src:pbyr is not null) THEN
                    'pbyr contains a non-numeric value : \'' || AS_VARCHAR(src:pbyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbyr_pbud_ref_compnr), '0'), 38, 10) is null and 
                    src:pbyr_pbud_ref_compnr is not null) THEN
                    'pbyr_pbud_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pbyr_pbud_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) THEN
                    'ratc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) THEN
                    'ratc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) THEN
                    'ratc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                                    
                src:year,
                src:compnr,
                src:budg  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFBS005 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:budg_ref_compnr), '0'), 38, 10) is null and 
                    src:budg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbud_ref_compnr), '0'), 38, 10) is null and 
                    src:cbud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbyr), '0'), 38, 10) is null and 
                    src:cbyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbyr_cbud_ref_compnr), '0'), 38, 10) is null and 
                    src:cbyr_cbud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbud_ref_compnr), '0'), 38, 10) is null and 
                    src:dbud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbyr), '0'), 38, 10) is null and 
                    src:dbyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbyr_dbud_ref_compnr), '0'), 38, 10) is null and 
                    src:dbyr_dbud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defi), '0'), 38, 10) is null and 
                    src:defi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disb_ref_compnr), '0'), 38, 10) is null and 
                    src:disb_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbud_ref_compnr), '0'), 38, 10) is null and 
                    src:pbud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbyr), '0'), 38, 10) is null and 
                    src:pbyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbyr_pbud_ref_compnr), '0'), 38, 10) is null and 
                    src:pbyr_pbud_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)