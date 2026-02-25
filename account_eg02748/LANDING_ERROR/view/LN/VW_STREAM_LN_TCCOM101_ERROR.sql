CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM101_ERROR AS SELECT src, 'LN_TCCOM101' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprl), '0'), 38, 10) is null and 
                    src:bprl is not null) THEN
                    'bprl contains a non-numeric value : \'' || AS_VARCHAR(src:bprl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btbv), '0'), 38, 10) is null and 
                    src:btbv is not null) THEN
                    'btbv contains a non-numeric value : \'' || AS_VARCHAR(src:btbv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfir), '0'), 38, 10) is null and 
                    src:cfir is not null) THEN
                    'cfir contains a non-numeric value : \'' || AS_VARCHAR(src:cfir) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) THEN
                    'cinm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cinm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crat_ref_compnr), '0'), 38, 10) is null and 
                    src:crat_ref_compnr is not null) THEN
                    'crat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) THEN
                    'ctyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcur_ref_compnr), '0'), 38, 10) is null and 
                    src:pcur_ref_compnr is not null) THEN
                    'pcur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scur_ref_compnr), '0'), 38, 10) is null and 
                    src:scur_ref_compnr is not null) THEN
                    'scur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:scur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfir), '0'), 38, 10) is null and 
                    src:sfir is not null) THEN
                    'sfir contains a non-numeric value : \'' || AS_VARCHAR(src:sfir) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spay_ref_compnr), '0'), 38, 10) is null and 
                    src:spay_ref_compnr is not null) THEN
                    'spay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:spay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) THEN
                    'styp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:styp_ref_compnr) || '\'' WHEN 
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
                                    
                src:sern,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM101 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprl), '0'), 38, 10) is null and 
                    src:bprl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btbv), '0'), 38, 10) is null and 
                    src:btbv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfir), '0'), 38, 10) is null and 
                    src:cfir is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crat_ref_compnr), '0'), 38, 10) is null and 
                    src:crat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcur_ref_compnr), '0'), 38, 10) is null and 
                    src:pcur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scur_ref_compnr), '0'), 38, 10) is null and 
                    src:scur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfir), '0'), 38, 10) is null and 
                    src:sfir is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spay_ref_compnr), '0'), 38, 10) is null and 
                    src:spay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is null and 
                    src:styp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)