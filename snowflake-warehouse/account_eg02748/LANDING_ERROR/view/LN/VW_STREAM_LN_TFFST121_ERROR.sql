CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFST121_ERROR AS SELECT src, 'LN_TFFST121' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpf), '0'), 38, 10) is null and 
                    src:cmpf is not null) THEN
                    'cmpf contains a non-numeric value : \'' || AS_VARCHAR(src:cmpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpt), '0'), 38, 10) is null and 
                    src:cmpt is not null) THEN
                    'cmpt contains a non-numeric value : \'' || AS_VARCHAR(src:cmpt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_accn_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_accn_ref_compnr is not null) THEN
                    'fstm_accn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_accn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_ref_compnr is not null) THEN
                    'fstm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
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
                                    
                src:fstm,
                src:seqn,
                src:compnr,
                src:accn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFST121 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpf), '0'), 38, 10) is null and 
                    src:cmpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpt), '0'), 38, 10) is null and 
                    src:cmpt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_accn_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_accn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)