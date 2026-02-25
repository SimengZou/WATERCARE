CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD007_ERROR AS SELECT src, 'LN_TFGLD007' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dacp), '1900-01-01')) is null and 
                    src:dacp is not null) THEN
                    'dacp contains a non-timestamp value : \'' || AS_VARCHAR(src:dacp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dacr), '1900-01-01')) is null and 
                    src:dacr is not null) THEN
                    'dacr contains a non-timestamp value : \'' || AS_VARCHAR(src:dacr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcmg), '1900-01-01')) is null and 
                    src:dcmg is not null) THEN
                    'dcmg contains a non-timestamp value : \'' || AS_VARCHAR(src:dcmg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dgld), '1900-01-01')) is null and 
                    src:dgld is not null) THEN
                    'dgld contains a non-timestamp value : \'' || AS_VARCHAR(src:dgld) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dint), '1900-01-01')) is null and 
                    src:dint is not null) THEN
                    'dint contains a non-timestamp value : \'' || AS_VARCHAR(src:dint) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno), '0'), 38, 10) is null and 
                    src:prno is not null) THEN
                    'prno contains a non-numeric value : \'' || AS_VARCHAR(src:prno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) THEN
                    'ptyp contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp_year_prno_ref_compnr), '0'), 38, 10) is null and 
                    src:ptyp_year_prno_ref_compnr is not null) THEN
                    'ptyp_year_prno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp_year_prno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacp), '0'), 38, 10) is null and 
                    src:sacp is not null) THEN
                    'sacp contains a non-numeric value : \'' || AS_VARCHAR(src:sacp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacr), '0'), 38, 10) is null and 
                    src:sacr is not null) THEN
                    'sacr contains a non-numeric value : \'' || AS_VARCHAR(src:sacr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmg), '0'), 38, 10) is null and 
                    src:scmg is not null) THEN
                    'scmg contains a non-numeric value : \'' || AS_VARCHAR(src:scmg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgld), '0'), 38, 10) is null and 
                    src:sgld is not null) THEN
                    'sgld contains a non-numeric value : \'' || AS_VARCHAR(src:sgld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sint), '0'), 38, 10) is null and 
                    src:sint is not null) THEN
                    'sint contains a non-numeric value : \'' || AS_VARCHAR(src:sint) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year_ref_compnr), '0'), 38, 10) is null and 
                    src:year_ref_compnr is not null) THEN
                    'year_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:year_ref_compnr) || '\'' WHEN 
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
                                    
                src:prno,
                src:compnr,
                src:year,
                src:ptyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD007 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dacp), '1900-01-01')) is null and 
                    src:dacp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dacr), '1900-01-01')) is null and 
                    src:dacr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcmg), '1900-01-01')) is null and 
                    src:dcmg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dgld), '1900-01-01')) is null and 
                    src:dgld is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dint), '1900-01-01')) is null and 
                    src:dint is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno), '0'), 38, 10) is null and 
                    src:prno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp_year_prno_ref_compnr), '0'), 38, 10) is null and 
                    src:ptyp_year_prno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacp), '0'), 38, 10) is null and 
                    src:sacp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacr), '0'), 38, 10) is null and 
                    src:sacr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scmg), '0'), 38, 10) is null and 
                    src:scmg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgld), '0'), 38, 10) is null and 
                    src:sgld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sint), '0'), 38, 10) is null and 
                    src:sint is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year_ref_compnr), '0'), 38, 10) is null and 
                    src:year_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)