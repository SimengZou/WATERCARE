CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM130_ERROR AS SELECT src, 'LN_TCCOM130' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afal_ref_compnr), '0'), 38, 10) is null and 
                    src:afal_ref_compnr is not null) THEN
                    'afal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:afal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cste_ccit_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cste_ccit_ref_compnr is not null) THEN
                    'ccty_cste_ccit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cste_ccit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cste_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cste_ref_compnr is not null) THEN
                    'ccty_cste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_cste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coaf_ref_compnr), '0'), 38, 10) is null and 
                    src:coaf_ref_compnr is not null) THEN
                    'coaf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:coaf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crid), '0'), 38, 10) is null and 
                    src:crid is not null) THEN
                    'crid contains a non-numeric value : \'' || AS_VARCHAR(src:crid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defa), '0'), 38, 10) is null and 
                    src:defa is not null) THEN
                    'defa contains a non-numeric value : \'' || AS_VARCHAR(src:defa) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dtlm), '1900-01-01')) is null and 
                    src:dtlm is not null) THEN
                    'dtlm contains a non-timestamp value : \'' || AS_VARCHAR(src:dtlm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp), '0'), 38, 10) is null and 
                    src:etyp is not null) THEN
                    'etyp contains a non-numeric value : \'' || AS_VARCHAR(src:etyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ezty_ref_compnr), '0'), 38, 10) is null and 
                    src:ezty_ref_compnr is not null) THEN
                    'ezty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ezty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:foid), '0'), 38, 10) is null and 
                    src:foid is not null) THEN
                    'foid contains a non-numeric value : \'' || AS_VARCHAR(src:foid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glat), '0'), 38, 10) is null and 
                    src:glat is not null) THEN
                    'glat contains a non-numeric value : \'' || AS_VARCHAR(src:glat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glon), '0'), 38, 10) is null and 
                    src:glon is not null) THEN
                    'glon contains a non-numeric value : \'' || AS_VARCHAR(src:glon) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lvdt), '1900-01-01')) is null and 
                    src:lvdt is not null) THEN
                    'lvdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lvdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is null and 
                    src:tzid_ref_compnr is not null) THEN
                    'tzid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tzid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:valr), '0'), 38, 10) is null and 
                    src:valr is not null) THEN
                    'valr contains a non-numeric value : \'' || AS_VARCHAR(src:valr) || '\'' WHEN 
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
                                    
                src:cadr,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM130 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afal_ref_compnr), '0'), 38, 10) is null and 
                    src:afal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cste_ccit_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cste_ccit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_cste_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_cste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coaf_ref_compnr), '0'), 38, 10) is null and 
                    src:coaf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crid), '0'), 38, 10) is null and 
                    src:crid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defa), '0'), 38, 10) is null and 
                    src:defa is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dtlm), '1900-01-01')) is null and 
                    src:dtlm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp), '0'), 38, 10) is null and 
                    src:etyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ezty_ref_compnr), '0'), 38, 10) is null and 
                    src:ezty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:foid), '0'), 38, 10) is null and 
                    src:foid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glat), '0'), 38, 10) is null and 
                    src:glat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glon), '0'), 38, 10) is null and 
                    src:glon is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lvdt), '1900-01-01')) is null and 
                    src:lvdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is null and 
                    src:tzid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:valr), '0'), 38, 10) is null and 
                    src:valr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)