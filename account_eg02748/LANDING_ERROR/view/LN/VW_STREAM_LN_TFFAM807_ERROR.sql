CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM807_ERROR AS SELECT src, 'LN_TFFAM807' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acqa_1), '0'), 38, 10) is null and 
                    src:acqa_1 is not null) THEN
                    'acqa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acqa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acqa_2), '0'), 38, 10) is null and 
                    src:acqa_2 is not null) THEN
                    'acqa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acqa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acqa_3), '0'), 38, 10) is null and 
                    src:acqa_3 is not null) THEN
                    'acqa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acqa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adja_1), '0'), 38, 10) is null and 
                    src:adja_1 is not null) THEN
                    'adja_1 contains a non-numeric value : \'' || AS_VARCHAR(src:adja_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adja_2), '0'), 38, 10) is null and 
                    src:adja_2 is not null) THEN
                    'adja_2 contains a non-numeric value : \'' || AS_VARCHAR(src:adja_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adja_3), '0'), 38, 10) is null and 
                    src:adja_3 is not null) THEN
                    'adja_3 contains a non-numeric value : \'' || AS_VARCHAR(src:adja_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is null and 
                    src:book_ref_compnr is not null) THEN
                    'book_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:book_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_1), '0'), 38, 10) is null and 
                    src:depr_1 is not null) THEN
                    'depr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_2), '0'), 38, 10) is null and 
                    src:depr_2 is not null) THEN
                    'depr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_3), '0'), 38, 10) is null and 
                    src:depr_3 is not null) THEN
                    'depr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:depr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa_1), '0'), 38, 10) is null and 
                    src:disa_1 is not null) THEN
                    'disa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:disa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa_2), '0'), 38, 10) is null and 
                    src:disa_2 is not null) THEN
                    'disa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:disa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa_3), '0'), 38, 10) is null and 
                    src:disa_3 is not null) THEN
                    'disa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:disa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkey), '0'), 38, 10) is null and 
                    src:lkey is not null) THEN
                    'lkey contains a non-numeric value : \'' || AS_VARCHAR(src:lkey) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proa_1), '0'), 38, 10) is null and 
                    src:proa_1 is not null) THEN
                    'proa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:proa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proa_2), '0'), 38, 10) is null and 
                    src:proa_2 is not null) THEN
                    'proa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:proa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proa_3), '0'), 38, 10) is null and 
                    src:proa_3 is not null) THEN
                    'proa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:proa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) THEN
                    'prod contains a non-numeric value : \'' || AS_VARCHAR(src:prod) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traa_1), '0'), 38, 10) is null and 
                    src:traa_1 is not null) THEN
                    'traa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:traa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traa_2), '0'), 38, 10) is null and 
                    src:traa_2 is not null) THEN
                    'traa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:traa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traa_3), '0'), 38, 10) is null and 
                    src:traa_3 is not null) THEN
                    'traa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:traa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trct_1), '0'), 38, 10) is null and 
                    src:trct_1 is not null) THEN
                    'trct_1 contains a non-numeric value : \'' || AS_VARCHAR(src:trct_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trct_2), '0'), 38, 10) is null and 
                    src:trct_2 is not null) THEN
                    'trct_2 contains a non-numeric value : \'' || AS_VARCHAR(src:trct_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trct_3), '0'), 38, 10) is null and 
                    src:trct_3 is not null) THEN
                    'trct_3 contains a non-numeric value : \'' || AS_VARCHAR(src:trct_3) || '\'' WHEN 
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
                                    
                src:acct,
                src:dim4,
                src:dim1,
                src:dims,
                src:prod,
                src:comp,
                src:dim5,
                src:compnr,
                src:idtc,
                src:dim2,
                src:dim3,
                src:year,
                src:book,
                src:lkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM807 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acqa_1), '0'), 38, 10) is null and 
                    src:acqa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acqa_2), '0'), 38, 10) is null and 
                    src:acqa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acqa_3), '0'), 38, 10) is null and 
                    src:acqa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adja_1), '0'), 38, 10) is null and 
                    src:adja_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adja_2), '0'), 38, 10) is null and 
                    src:adja_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adja_3), '0'), 38, 10) is null and 
                    src:adja_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is null and 
                    src:book_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_1), '0'), 38, 10) is null and 
                    src:depr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_2), '0'), 38, 10) is null and 
                    src:depr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depr_3), '0'), 38, 10) is null and 
                    src:depr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa_1), '0'), 38, 10) is null and 
                    src:disa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa_2), '0'), 38, 10) is null and 
                    src:disa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa_3), '0'), 38, 10) is null and 
                    src:disa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkey), '0'), 38, 10) is null and 
                    src:lkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proa_1), '0'), 38, 10) is null and 
                    src:proa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proa_2), '0'), 38, 10) is null and 
                    src:proa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proa_3), '0'), 38, 10) is null and 
                    src:proa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traa_1), '0'), 38, 10) is null and 
                    src:traa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traa_2), '0'), 38, 10) is null and 
                    src:traa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:traa_3), '0'), 38, 10) is null and 
                    src:traa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trct_1), '0'), 38, 10) is null and 
                    src:trct_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trct_2), '0'), 38, 10) is null and 
                    src:trct_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trct_3), '0'), 38, 10) is null and 
                    src:trct_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)