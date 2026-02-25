CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM016_ERROR AS SELECT src, 'LN_TPPDM016' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) THEN
                    'avtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:avtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) THEN
                    'clrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcpu), '0'), 38, 10) is null and 
                    src:dcpu is not null) THEN
                    'dcpu contains a non-numeric value : \'' || AS_VARCHAR(src:dcpu) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltlr), '1900-01-01')) is null and 
                    src:ltlr is not null) THEN
                    'ltlr contains a non-timestamp value : \'' || AS_VARCHAR(src:ltlr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mane), '0'), 38, 10) is null and 
                    src:mane is not null) THEN
                    'mane contains a non-numeric value : \'' || AS_VARCHAR(src:mane) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mine), '0'), 38, 10) is null and 
                    src:mine is not null) THEN
                    'mine contains a non-numeric value : \'' || AS_VARCHAR(src:mine) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtrg_ref_compnr), '0'), 38, 10) is null and 
                    src:mtrg_ref_compnr is not null) THEN
                    'mtrg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mtrg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuem), '0'), 38, 10) is null and 
                    src:nuem is not null) THEN
                    'nuem contains a non-numeric value : \'' || AS_VARCHAR(src:nuem) || '\'' WHEN 
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
                                    
                src:ctrg,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM016 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcpu), '0'), 38, 10) is null and 
                    src:dcpu is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltlr), '1900-01-01')) is null and 
                    src:ltlr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mane), '0'), 38, 10) is null and 
                    src:mane is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mine), '0'), 38, 10) is null and 
                    src:mine is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtrg_ref_compnr), '0'), 38, 10) is null and 
                    src:mtrg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuem), '0'), 38, 10) is null and 
                    src:nuem is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)