CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM120_ERROR AS SELECT src, 'LN_TFFAM120' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_ref_compnr is not null) THEN
                    'anbr_aext_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:anbr_aext_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dkey), '0'), 38, 10) is null and 
                    src:dkey is not null) THEN
                    'dkey contains a non-numeric value : \'' || AS_VARCHAR(src:dkey) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dqty), '0'), 38, 10) is null and 
                    src:dqty is not null) THEN
                    'dqty contains a non-numeric value : \'' || AS_VARCHAR(src:dqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkey), '0'), 38, 10) is null and 
                    src:lkey is not null) THEN
                    'lkey contains a non-numeric value : \'' || AS_VARCHAR(src:lkey) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srce), '0'), 38, 10) is null and 
                    src:srce is not null) THEN
                    'srce contains a non-numeric value : \'' || AS_VARCHAR(src:srce) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                                    
                src:anbr,
                src:aext,
                src:compnr,
                src:dkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM120 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is null and 
                    src:anbr_aext_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dkey), '0'), 38, 10) is null and 
                    src:dkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dqty), '0'), 38, 10) is null and 
                    src:dqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkey), '0'), 38, 10) is null and 
                    src:lkey is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srce), '0'), 38, 10) is null and 
                    src:srce is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)