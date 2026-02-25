CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSMDM140_ERROR AS SELECT src, 'LN_TSMDM140' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asmc_ref_compnr), '0'), 38, 10) is null and 
                    src:asmc_ref_compnr is not null) THEN
                    'asmc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:asmc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccar_ref_compnr), '0'), 38, 10) is null and 
                    src:ccar_ref_compnr is not null) THEN
                    'ccar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) THEN
                    'csar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frmn_ref_compnr), '0'), 38, 10) is null and 
                    src:frmn_ref_compnr is not null) THEN
                    'frmn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:frmn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lsdt), '1900-01-01')) is null and 
                    src:lsdt is not null) THEN
                    'lsdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lsdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopd), '0'), 38, 10) is null and 
                    src:mopd is not null) THEN
                    'mopd contains a non-numeric value : \'' || AS_VARCHAR(src:mopd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgen), '0'), 38, 10) is null and 
                    src:pgen is not null) THEN
                    'pgen contains a non-numeric value : \'' || AS_VARCHAR(src:pgen) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucts), '0'), 38, 10) is null and 
                    src:ucts is not null) THEN
                    'ucts contains a non-numeric value : \'' || AS_VARCHAR(src:ucts) || '\'' WHEN 
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
                src:emno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSMDM140 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asmc_ref_compnr), '0'), 38, 10) is null and 
                    src:asmc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccar_ref_compnr), '0'), 38, 10) is null and 
                    src:ccar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is null and 
                    src:csar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frmn_ref_compnr), '0'), 38, 10) is null and 
                    src:frmn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lsdt), '1900-01-01')) is null and 
                    src:lsdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopd), '0'), 38, 10) is null and 
                    src:mopd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgen), '0'), 38, 10) is null and 
                    src:pgen is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucts), '0'), 38, 10) is null and 
                    src:ucts is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)