CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM808_ERROR AS SELECT src, 'LN_TFFAM808' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_1), '0'), 38, 10) is null and 
                    src:ocst_1 is not null) THEN
                    'ocst_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ocst_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_2), '0'), 38, 10) is null and 
                    src:ocst_2 is not null) THEN
                    'ocst_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ocst_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_3), '0'), 38, 10) is null and 
                    src:ocst_3 is not null) THEN
                    'ocst_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ocst_3) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_1), '0'), 38, 10) is null and 
                    src:s179_1 is not null) THEN
                    's179_1 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_2), '0'), 38, 10) is null and 
                    src:s179_2 is not null) THEN
                    's179_2 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_3), '0'), 38, 10) is null and 
                    src:s179_3 is not null) THEN
                    's179_3 contains a non-numeric value : \'' || AS_VARCHAR(src:s179_3) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susp), '0'), 38, 10) is null and 
                    src:susp is not null) THEN
                    'susp contains a non-numeric value : \'' || AS_VARCHAR(src:susp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_1), '0'), 38, 10) is null and 
                    src:ytdd_1 is not null) THEN
                    'ytdd_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdd_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_2), '0'), 38, 10) is null and 
                    src:ytdd_2 is not null) THEN
                    'ytdd_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdd_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_3), '0'), 38, 10) is null and 
                    src:ytdd_3 is not null) THEN
                    'ytdd_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdd_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_1), '0'), 38, 10) is null and 
                    src:ytdr_1 is not null) THEN
                    'ytdr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_2), '0'), 38, 10) is null and 
                    src:ytdr_2 is not null) THEN
                    'ytdr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_3), '0'), 38, 10) is null and 
                    src:ytdr_3 is not null) THEN
                    'ytdr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ytdr_3) || '\'' WHEN 
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
                src:year,
                src:anbr,
                src:aext,
                src:prod,
                src:book  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM808 as strm)
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_1), '0'), 38, 10) is null and 
                    src:ocst_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_2), '0'), 38, 10) is null and 
                    src:ocst_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocst_3), '0'), 38, 10) is null and 
                    src:ocst_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_1), '0'), 38, 10) is null and 
                    src:rcst_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_2), '0'), 38, 10) is null and 
                    src:rcst_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcst_3), '0'), 38, 10) is null and 
                    src:rcst_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_1), '0'), 38, 10) is null and 
                    src:s179_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_2), '0'), 38, 10) is null and 
                    src:s179_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179_3), '0'), 38, 10) is null and 
                    src:s179_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_1), '0'), 38, 10) is null and 
                    src:salv_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_2), '0'), 38, 10) is null and 
                    src:salv_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv_3), '0'), 38, 10) is null and 
                    src:salv_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:susp), '0'), 38, 10) is null and 
                    src:susp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_1), '0'), 38, 10) is null and 
                    src:ytdd_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_2), '0'), 38, 10) is null and 
                    src:ytdd_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdd_3), '0'), 38, 10) is null and 
                    src:ytdd_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_1), '0'), 38, 10) is null and 
                    src:ytdr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_2), '0'), 38, 10) is null and 
                    src:ytdr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ytdr_3), '0'), 38, 10) is null and 
                    src:ytdr_3 is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)