CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM005_ERROR AS SELECT src, 'LN_TPPDM005' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) THEN
                    'ccfu contains a non-numeric value : \'' || AS_VARCHAR(src:ccfu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copt), '0'), 38, 10) is null and 
                    src:copt is not null) THEN
                    'copt contains a non-numeric value : \'' || AS_VARCHAR(src:copt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpmc_1), '0'), 38, 10) is null and 
                    src:cpmc_1 is not null) THEN
                    'cpmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpmc_2), '0'), 38, 10) is null and 
                    src:cpmc_2 is not null) THEN
                    'cpmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpmc_3), '0'), 38, 10) is null and 
                    src:cpmc_3 is not null) THEN
                    'cpmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cpmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cppp), '0'), 38, 10) is null and 
                    src:cppp is not null) THEN
                    'cppp contains a non-numeric value : \'' || AS_VARCHAR(src:cppp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprp), '0'), 38, 10) is null and 
                    src:cprp is not null) THEN
                    'cprp contains a non-numeric value : \'' || AS_VARCHAR(src:cprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltcp), '1900-01-01')) is null and 
                    src:ltcp is not null) THEN
                    'ltcp contains a non-timestamp value : \'' || AS_VARCHAR(src:ltcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) THEN
                    'osys contains a non-numeric value : \'' || AS_VARCHAR(src:osys) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgwh), '0'), 38, 10) is null and 
                    src:pgwh is not null) THEN
                    'pgwh contains a non-numeric value : \'' || AS_VARCHAR(src:pgwh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) THEN
                    'prre contains a non-numeric value : \'' || AS_VARCHAR(src:prre) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usyn), '0'), 38, 10) is null and 
                    src:usyn is not null) THEN
                    'usyn contains a non-numeric value : \'' || AS_VARCHAR(src:usyn) || '\'' WHEN 
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
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM005 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copt), '0'), 38, 10) is null and 
                    src:copt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpmc_1), '0'), 38, 10) is null and 
                    src:cpmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpmc_2), '0'), 38, 10) is null and 
                    src:cpmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpmc_3), '0'), 38, 10) is null and 
                    src:cpmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cppp), '0'), 38, 10) is null and 
                    src:cppp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprp), '0'), 38, 10) is null and 
                    src:cprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltcp), '1900-01-01')) is null and 
                    src:ltcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgwh), '0'), 38, 10) is null and 
                    src:pgwh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usyn), '0'), 38, 10) is null and 
                    src:usyn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)