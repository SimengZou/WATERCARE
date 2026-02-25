CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCGEN000_ERROR AS SELECT src, 'LN_TCGEN000' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awdt), '0'), 38, 10) is null and 
                    src:awdt is not null) THEN
                    'awdt contains a non-numeric value : \'' || AS_VARCHAR(src:awdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awfa), '0'), 38, 10) is null and 
                    src:awfa is not null) THEN
                    'awfa contains a non-numeric value : \'' || AS_VARCHAR(src:awfa) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:clcd), '1900-01-01')) is null and 
                    src:clcd is not null) THEN
                    'clcd contains a non-timestamp value : \'' || AS_VARCHAR(src:clcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpie_ref_compnr), '0'), 38, 10) is null and 
                    src:cpie_ref_compnr is not null) THEN
                    'cpie_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpie_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpd), '0'), 38, 10) is null and 
                    src:crpd is not null) THEN
                    'crpd contains a non-numeric value : \'' || AS_VARCHAR(src:crpd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvfd), '1900-01-01')) is null and 
                    src:cvfd is not null) THEN
                    'cvfd contains a non-timestamp value : \'' || AS_VARCHAR(src:cvfd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcur_ref_compnr), '0'), 38, 10) is null and 
                    src:dcur_ref_compnr is not null) THEN
                    'dcur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dcur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:time), '0'), 38, 10) is null and 
                    src:time is not null) THEN
                    'time contains a non-numeric value : \'' || AS_VARCHAR(src:time) || '\'' WHEN 
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
                                    
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCGEN000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awdt), '0'), 38, 10) is null and 
                    src:awdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awfa), '0'), 38, 10) is null and 
                    src:awfa is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:clcd), '1900-01-01')) is null and 
                    src:clcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpie_ref_compnr), '0'), 38, 10) is null and 
                    src:cpie_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crpd), '0'), 38, 10) is null and 
                    src:crpd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cvfd), '1900-01-01')) is null and 
                    src:cvfd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcur_ref_compnr), '0'), 38, 10) is null and 
                    src:dcur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:time), '0'), 38, 10) is null and 
                    src:time is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)