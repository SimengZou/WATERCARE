CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TICST010_ERROR AS SELECT src, 'LN_TICST010' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_1), '0'), 38, 10) is null and 
                    src:aamt_1 is not null) THEN
                    'aamt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_2), '0'), 38, 10) is null and 
                    src:aamt_2 is not null) THEN
                    'aamt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_3), '0'), 38, 10) is null and 
                    src:aamt_3 is not null) THEN
                    'aamt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_dwhc), '0'), 38, 10) is null and 
                    src:aamt_dwhc is not null) THEN
                    'aamt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_lclc), '0'), 38, 10) is null and 
                    src:aamt_lclc is not null) THEN
                    'aamt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_refc), '0'), 38, 10) is null and 
                    src:aamt_refc is not null) THEN
                    'aamt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc1), '0'), 38, 10) is null and 
                    src:aamt_rpc1 is not null) THEN
                    'aamt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc2), '0'), 38, 10) is null and 
                    src:aamt_rpc2 is not null) THEN
                    'aamt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:aamt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addc), '0'), 38, 10) is null and 
                    src:addc is not null) THEN
                    'addc contains a non-numeric value : \'' || AS_VARCHAR(src:addc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) THEN
                    'cpcp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csto), '0'), 38, 10) is null and 
                    src:csto is not null) THEN
                    'csto contains a non-numeric value : \'' || AS_VARCHAR(src:csto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is null and 
                    src:dptm_fcmp is not null) THEN
                    'dptm_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:dptm_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_1), '0'), 38, 10) is null and 
                    src:eamt_1 is not null) THEN
                    'eamt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_2), '0'), 38, 10) is null and 
                    src:eamt_2 is not null) THEN
                    'eamt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_3), '0'), 38, 10) is null and 
                    src:eamt_3 is not null) THEN
                    'eamt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_dwhc), '0'), 38, 10) is null and 
                    src:eamt_dwhc is not null) THEN
                    'eamt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_lclc), '0'), 38, 10) is null and 
                    src:eamt_lclc is not null) THEN
                    'eamt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_refc), '0'), 38, 10) is null and 
                    src:eamt_refc is not null) THEN
                    'eamt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_rpc1), '0'), 38, 10) is null and 
                    src:eamt_rpc1 is not null) THEN
                    'eamt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_rpc2), '0'), 38, 10) is null and 
                    src:eamt_rpc2 is not null) THEN
                    'eamt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:eamt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuna), '0'), 38, 10) is null and 
                    src:nuna is not null) THEN
                    'nuna contains a non-numeric value : \'' || AS_VARCHAR(src:nuna) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nune), '0'), 38, 10) is null and 
                    src:nune is not null) THEN
                    'nune contains a non-numeric value : \'' || AS_VARCHAR(src:nune) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) THEN
                    'pdno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pdno_ref_compnr) || '\'' WHEN 
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
                                    
                src:pdno,
                src:cpcp,
                src:cstv,
                src:addc,
                src:cwoc,
                src:csto,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TICST010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_1), '0'), 38, 10) is null and 
                    src:aamt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_2), '0'), 38, 10) is null and 
                    src:aamt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_3), '0'), 38, 10) is null and 
                    src:aamt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_dwhc), '0'), 38, 10) is null and 
                    src:aamt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_lclc), '0'), 38, 10) is null and 
                    src:aamt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_refc), '0'), 38, 10) is null and 
                    src:aamt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc1), '0'), 38, 10) is null and 
                    src:aamt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aamt_rpc2), '0'), 38, 10) is null and 
                    src:aamt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addc), '0'), 38, 10) is null and 
                    src:addc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csto), '0'), 38, 10) is null and 
                    src:csto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is null and 
                    src:dptm_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_1), '0'), 38, 10) is null and 
                    src:eamt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_2), '0'), 38, 10) is null and 
                    src:eamt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_3), '0'), 38, 10) is null and 
                    src:eamt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_dwhc), '0'), 38, 10) is null and 
                    src:eamt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_lclc), '0'), 38, 10) is null and 
                    src:eamt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_refc), '0'), 38, 10) is null and 
                    src:eamt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_rpc1), '0'), 38, 10) is null and 
                    src:eamt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt_rpc2), '0'), 38, 10) is null and 
                    src:eamt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuna), '0'), 38, 10) is null and 
                    src:nuna is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nune), '0'), 38, 10) is null and 
                    src:nune is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)