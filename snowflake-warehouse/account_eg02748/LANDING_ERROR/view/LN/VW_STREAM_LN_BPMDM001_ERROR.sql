CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_BPMDM001_ERROR AS SELECT src, 'LN_BPMDM001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cedt), '1900-01-01')) is null and 
                    src:cedt is not null) THEN
                    'cedt contains a non-timestamp value : \'' || AS_VARCHAR(src:cedt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:daob), '1900-01-01')) is null and 
                    src:daob is not null) THEN
                    'daob contains a non-timestamp value : \'' || AS_VARCHAR(src:daob) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edte), '1900-01-01')) is null and 
                    src:edte is not null) THEN
                    'edte contains a non-timestamp value : \'' || AS_VARCHAR(src:edte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emtp), '0'), 38, 10) is null and 
                    src:emtp is not null) THEN
                    'emtp contains a non-numeric value : \'' || AS_VARCHAR(src:emtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hwem), '0'), 38, 10) is null and 
                    src:hwem is not null) THEN
                    'hwem contains a non-numeric value : \'' || AS_VARCHAR(src:hwem) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:msty), '0'), 38, 10) is null and 
                    src:msty is not null) THEN
                    'msty contains a non-numeric value : \'' || AS_VARCHAR(src:msty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdte), '1900-01-01')) is null and 
                    src:sdte is not null) THEN
                    'sdte contains a non-timestamp value : \'' || AS_VARCHAR(src:sdte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sexe), '0'), 38, 10) is null and 
                    src:sexe is not null) THEN
                    'sexe contains a non-numeric value : \'' || AS_VARCHAR(src:sexe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:supv_ref_compnr), '0'), 38, 10) is null and 
                    src:supv_ref_compnr is not null) THEN
                    'supv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:supv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucrm), '0'), 38, 10) is null and 
                    src:ucrm is not null) THEN
                    'ucrm contains a non-numeric value : \'' || AS_VARCHAR(src:ucrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtsc_ref_compnr), '0'), 38, 10) is null and 
                    src:wtsc_ref_compnr is not null) THEN
                    'wtsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wtsc_ref_compnr) || '\'' WHEN 
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
                                    
                src:emno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_BPMDM001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cedt), '1900-01-01')) is null and 
                    src:cedt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:daob), '1900-01-01')) is null and 
                    src:daob is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edte), '1900-01-01')) is null and 
                    src:edte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emtp), '0'), 38, 10) is null and 
                    src:emtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hwem), '0'), 38, 10) is null and 
                    src:hwem is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:msty), '0'), 38, 10) is null and 
                    src:msty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdte), '1900-01-01')) is null and 
                    src:sdte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sexe), '0'), 38, 10) is null and 
                    src:sexe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:supv_ref_compnr), '0'), 38, 10) is null and 
                    src:supv_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucrm), '0'), 38, 10) is null and 
                    src:ucrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtsc_ref_compnr), '0'), 38, 10) is null and 
                    src:wtsc_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)