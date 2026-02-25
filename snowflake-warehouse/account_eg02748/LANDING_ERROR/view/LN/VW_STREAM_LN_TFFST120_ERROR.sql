CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFST120_ERROR AS SELECT src, 'LN_TFFST120' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:accs), '0'), 38, 10) is null and 
                    src:accs is not null) THEN
                    'accs contains a non-numeric value : \'' || AS_VARCHAR(src:accs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acct), '0'), 38, 10) is null and 
                    src:acct is not null) THEN
                    'acct contains a non-numeric value : \'' || AS_VARCHAR(src:acct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt), '0'), 38, 10) is null and 
                    src:atxt is not null) THEN
                    'atxt contains a non-numeric value : \'' || AS_VARCHAR(src:atxt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt_ref_compnr), '0'), 38, 10) is null and 
                    src:atxt_ref_compnr is not null) THEN
                    'atxt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atxt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfsa), '0'), 38, 10) is null and 
                    src:cfsa is not null) THEN
                    'cfsa contains a non-numeric value : \'' || AS_VARCHAR(src:cfsa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcgl), '0'), 38, 10) is null and 
                    src:dcgl is not null) THEN
                    'dcgl contains a non-numeric value : \'' || AS_VARCHAR(src:dcgl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcsw), '0'), 38, 10) is null and 
                    src:dcsw is not null) THEN
                    'dcsw contains a non-numeric value : \'' || AS_VARCHAR(src:dcsw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_acca_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_acca_ref_compnr is not null) THEN
                    'fstm_acca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_acca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_acrd_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_acrd_ref_compnr is not null) THEN
                    'fstm_acrd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_acrd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_cgla_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_cgla_ref_compnr is not null) THEN
                    'fstm_cgla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_cgla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_facc_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_facc_ref_compnr is not null) THEN
                    'fstm_facc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_facc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_pacc_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_pacc_ref_compnr is not null) THEN
                    'fstm_pacc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_pacc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_ref_compnr is not null) THEN
                    'fstm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fstm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:muaa), '0'), 38, 10) is null and 
                    src:muaa is not null) THEN
                    'muaa contains a non-numeric value : \'' || AS_VARCHAR(src:muaa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prta), '0'), 38, 10) is null and 
                    src:prta is not null) THEN
                    'prta contains a non-numeric value : \'' || AS_VARCHAR(src:prta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhis), '0'), 38, 10) is null and 
                    src:rhis is not null) THEN
                    'rhis contains a non-numeric value : \'' || AS_VARCHAR(src:rhis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sign), '0'), 38, 10) is null and 
                    src:sign is not null) THEN
                    'sign contains a non-numeric value : \'' || AS_VARCHAR(src:sign) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) THEN
                    'subl contains a non-numeric value : \'' || AS_VARCHAR(src:subl) || '\'' WHEN 
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
                                    
                src:compnr,
                src:fstm,
                src:accn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFST120 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:accs), '0'), 38, 10) is null and 
                    src:accs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acct), '0'), 38, 10) is null and 
                    src:acct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt), '0'), 38, 10) is null and 
                    src:atxt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt_ref_compnr), '0'), 38, 10) is null and 
                    src:atxt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfsa), '0'), 38, 10) is null and 
                    src:cfsa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcgl), '0'), 38, 10) is null and 
                    src:dcgl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcsw), '0'), 38, 10) is null and 
                    src:dcsw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_acca_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_acca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_acrd_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_acrd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_cgla_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_cgla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_facc_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_facc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_pacc_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_pacc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstm_ref_compnr), '0'), 38, 10) is null and 
                    src:fstm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:muaa), '0'), 38, 10) is null and 
                    src:muaa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prta), '0'), 38, 10) is null and 
                    src:prta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhis), '0'), 38, 10) is null and 
                    src:rhis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sign), '0'), 38, 10) is null and 
                    src:sign is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)