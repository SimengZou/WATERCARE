CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TIROU101_ERROR AS SELECT src, 'LN_TIROU101' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwca_ref_compnr), '0'), 38, 10) is null and 
                    src:bwca_ref_compnr is not null) THEN
                    'bwca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bwca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwce_ref_compnr), '0'), 38, 10) is null and 
                    src:bwce_ref_compnr is not null) THEN
                    'bwce_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bwce_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdr), '1900-01-01')) is null and 
                    src:lcdr is not null) THEN
                    'lcdr contains a non-timestamp value : \'' || AS_VARCHAR(src:lcdr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_ref_compnr), '0'), 38, 10) is null and 
                    src:mitm_ref_compnr is not null) THEN
                    'mitm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mitm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrau), '0'), 38, 10) is null and 
                    src:mrau is not null) THEN
                    'mrau contains a non-numeric value : \'' || AS_VARCHAR(src:mrau) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract), '0'), 38, 10) is null and 
                    src:ract is not null) THEN
                    'ract contains a non-numeric value : \'' || AS_VARCHAR(src:ract) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcal), '0'), 38, 10) is null and 
                    src:rcal is not null) THEN
                    'rcal contains a non-numeric value : \'' || AS_VARCHAR(src:rcal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) THEN
                    'runi contains a non-numeric value : \'' || AS_VARCHAR(src:runi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ruoq), '0'), 38, 10) is null and 
                    src:ruoq is not null) THEN
                    'ruoq contains a non-numeric value : \'' || AS_VARCHAR(src:ruoq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcf), '0'), 38, 10) is null and 
                    src:stcf is not null) THEN
                    'stcf contains a non-numeric value : \'' || AS_VARCHAR(src:stcf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stor), '0'), 38, 10) is null and 
                    src:stor is not null) THEN
                    'stor contains a non-numeric value : \'' || AS_VARCHAR(src:stor) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unef), '0'), 38, 10) is null and 
                    src:unef is not null) THEN
                    'unef contains a non-numeric value : \'' || AS_VARCHAR(src:unef) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wkbt), '0'), 38, 10) is null and 
                    src:wkbt is not null) THEN
                    'wkbt contains a non-numeric value : \'' || AS_VARCHAR(src:wkbt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:woar_ref_compnr), '0'), 38, 10) is null and 
                    src:woar_ref_compnr is not null) THEN
                    'woar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:woar_ref_compnr) || '\'' WHEN 
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
                                    
                src:opro,
                src:compnr,
                src:mitm  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIROU101 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwca_ref_compnr), '0'), 38, 10) is null and 
                    src:bwca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bwce_ref_compnr), '0'), 38, 10) is null and 
                    src:bwce_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdr), '1900-01-01')) is null and 
                    src:lcdr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitm_ref_compnr), '0'), 38, 10) is null and 
                    src:mitm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrau), '0'), 38, 10) is null and 
                    src:mrau is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract), '0'), 38, 10) is null and 
                    src:ract is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcal), '0'), 38, 10) is null and 
                    src:rcal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ruoq), '0'), 38, 10) is null and 
                    src:ruoq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcf), '0'), 38, 10) is null and 
                    src:stcf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stor), '0'), 38, 10) is null and 
                    src:stor is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unef), '0'), 38, 10) is null and 
                    src:unef is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wkbt), '0'), 38, 10) is null and 
                    src:wkbt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:woar_ref_compnr), '0'), 38, 10) is null and 
                    src:woar_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)