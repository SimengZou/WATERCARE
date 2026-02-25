CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCMCS010_ERROR AS SELECT src, 'LN_TCMCS010' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afal_ref_compnr), '0'), 38, 10) is null and 
                    src:afal_ref_compnr is not null) THEN
                    'afal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:afal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awtn), '0'), 38, 10) is null and 
                    src:awtn is not null) THEN
                    'awtn contains a non-numeric value : \'' || AS_VARCHAR(src:awtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bnch), '0'), 38, 10) is null and 
                    src:bnch is not null) THEN
                    'bnch contains a non-numeric value : \'' || AS_VARCHAR(src:bnch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgp1_ref_compnr), '0'), 38, 10) is null and 
                    src:cgp1_ref_compnr is not null) THEN
                    'cgp1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cgp1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgp2_ref_compnr), '0'), 38, 10) is null and 
                    src:cgp2_ref_compnr is not null) THEN
                    'cgp2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cgp2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coaf_ref_compnr), '0'), 38, 10) is null and 
                    src:coaf_ref_compnr is not null) THEN
                    'coaf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:coaf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:meec), '0'), 38, 10) is null and 
                    src:meec is not null) THEN
                    'meec contains a non-numeric value : \'' || AS_VARCHAR(src:meec) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pltx), '0'), 38, 10) is null and 
                    src:pltx is not null) THEN
                    'pltx contains a non-numeric value : \'' || AS_VARCHAR(src:pltx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppcd), '0'), 38, 10) is null and 
                    src:ppcd is not null) THEN
                    'ppcd contains a non-numeric value : \'' || AS_VARCHAR(src:ppcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptta), '0'), 38, 10) is null and 
                    src:ptta is not null) THEN
                    'ptta contains a non-numeric value : \'' || AS_VARCHAR(src:ptta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txmp), '0'), 38, 10) is null and 
                    src:txmp is not null) THEN
                    'txmp contains a non-numeric value : \'' || AS_VARCHAR(src:txmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is null and 
                    src:tzid_ref_compnr is not null) THEN
                    'tzid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tzid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uarc), '0'), 38, 10) is null and 
                    src:uarc is not null) THEN
                    'uarc contains a non-numeric value : \'' || AS_VARCHAR(src:uarc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vnch), '0'), 38, 10) is null and 
                    src:vnch is not null) THEN
                    'vnch contains a non-numeric value : \'' || AS_VARCHAR(src:vnch) || '\'' WHEN 
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
                src:ccty  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCMCS010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afal_ref_compnr), '0'), 38, 10) is null and 
                    src:afal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awtn), '0'), 38, 10) is null and 
                    src:awtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bnch), '0'), 38, 10) is null and 
                    src:bnch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgp1_ref_compnr), '0'), 38, 10) is null and 
                    src:cgp1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgp2_ref_compnr), '0'), 38, 10) is null and 
                    src:cgp2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coaf_ref_compnr), '0'), 38, 10) is null and 
                    src:coaf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:meec), '0'), 38, 10) is null and 
                    src:meec is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pltx), '0'), 38, 10) is null and 
                    src:pltx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppcd), '0'), 38, 10) is null and 
                    src:ppcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptta), '0'), 38, 10) is null and 
                    src:ptta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txmp), '0'), 38, 10) is null and 
                    src:txmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is null and 
                    src:tzid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uarc), '0'), 38, 10) is null and 
                    src:uarc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vnch), '0'), 38, 10) is null and 
                    src:vnch is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)