CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFST100_ERROR AS SELECT src, 'LN_TFFST100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:accs), '0'), 38, 10) is null and 
                    src:accs is not null) THEN
                    'accs contains a non-numeric value : \'' || AS_VARCHAR(src:accs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altp), '0'), 38, 10) is null and 
                    src:altp is not null) THEN
                    'altp contains a non-numeric value : \'' || AS_VARCHAR(src:altp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altp_salt_ref_compnr), '0'), 38, 10) is null and 
                    src:altp_salt_ref_compnr is not null) THEN
                    'altp_salt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:altp_salt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anhe), '0'), 38, 10) is null and 
                    src:anhe is not null) THEN
                    'anhe contains a non-numeric value : \'' || AS_VARCHAR(src:anhe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anhe_ref_compnr), '0'), 38, 10) is null and 
                    src:anhe_ref_compnr is not null) THEN
                    'anhe_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:anhe_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cagr_ref_compnr), '0'), 38, 10) is null and 
                    src:cagr_ref_compnr is not null) THEN
                    'cagr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cagr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cugl), '0'), 38, 10) is null and 
                    src:cugl is not null) THEN
                    'cugl contains a non-numeric value : \'' || AS_VARCHAR(src:cugl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:datc), '1900-01-01')) is null and 
                    src:datc is not null) THEN
                    'datc contains a non-timestamp value : \'' || AS_VARCHAR(src:datc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:datm), '1900-01-01')) is null and 
                    src:datm is not null) THEN
                    'datm contains a non-timestamp value : \'' || AS_VARCHAR(src:datm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mdfs), '1900-01-01')) is null and 
                    src:mdfs is not null) THEN
                    'mdfs contains a non-timestamp value : \'' || AS_VARCHAR(src:mdfs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nstm_ref_compnr), '0'), 38, 10) is null and 
                    src:nstm_ref_compnr is not null) THEN
                    'nstm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:nstm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ouer), '0'), 38, 10) is null and 
                    src:ouer is not null) THEN
                    'ouer contains a non-numeric value : \'' || AS_VARCHAR(src:ouer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pann), '0'), 38, 10) is null and 
                    src:pann is not null) THEN
                    'pann contains a non-numeric value : \'' || AS_VARCHAR(src:pann) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptra), '0'), 38, 10) is null and 
                    src:ptra is not null) THEN
                    'ptra contains a non-numeric value : \'' || AS_VARCHAR(src:ptra) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcur_ref_compnr), '0'), 38, 10) is null and 
                    src:rcur_ref_compnr is not null) THEN
                    'rcur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhis), '0'), 38, 10) is null and 
                    src:rhis is not null) THEN
                    'rhis contains a non-numeric value : \'' || AS_VARCHAR(src:rhis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltp), '0'), 38, 10) is null and 
                    src:sltp is not null) THEN
                    'sltp contains a non-numeric value : \'' || AS_VARCHAR(src:sltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltp_stlt_ref_compnr), '0'), 38, 10) is null and 
                    src:sltp_stlt_ref_compnr is not null) THEN
                    'sltp_stlt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sltp_stlt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sthe), '0'), 38, 10) is null and 
                    src:sthe is not null) THEN
                    'sthe contains a non-numeric value : \'' || AS_VARCHAR(src:sthe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sthe_ref_compnr), '0'), 38, 10) is null and 
                    src:sthe_ref_compnr is not null) THEN
                    'sthe_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sthe_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) THEN
                    'styp contains a non-numeric value : \'' || AS_VARCHAR(src:styp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:txdt), '1900-01-01')) is null and 
                    src:txdt is not null) THEN
                    'txdt contains a non-timestamp value : \'' || AS_VARCHAR(src:txdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vers), '0'), 38, 10) is null and 
                    src:vers is not null) THEN
                    'vers contains a non-numeric value : \'' || AS_VARCHAR(src:vers) || '\'' WHEN 
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
                src:fstm  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFST100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:accs), '0'), 38, 10) is null and 
                    src:accs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altp), '0'), 38, 10) is null and 
                    src:altp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altp_salt_ref_compnr), '0'), 38, 10) is null and 
                    src:altp_salt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anhe), '0'), 38, 10) is null and 
                    src:anhe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:anhe_ref_compnr), '0'), 38, 10) is null and 
                    src:anhe_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cagr_ref_compnr), '0'), 38, 10) is null and 
                    src:cagr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cugl), '0'), 38, 10) is null and 
                    src:cugl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:datc), '1900-01-01')) is null and 
                    src:datc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:datm), '1900-01-01')) is null and 
                    src:datm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mdfs), '1900-01-01')) is null and 
                    src:mdfs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nstm_ref_compnr), '0'), 38, 10) is null and 
                    src:nstm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ouer), '0'), 38, 10) is null and 
                    src:ouer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pann), '0'), 38, 10) is null and 
                    src:pann is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptra), '0'), 38, 10) is null and 
                    src:ptra is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcur_ref_compnr), '0'), 38, 10) is null and 
                    src:rcur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rhis), '0'), 38, 10) is null and 
                    src:rhis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltp), '0'), 38, 10) is null and 
                    src:sltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltp_stlt_ref_compnr), '0'), 38, 10) is null and 
                    src:sltp_stlt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sthe), '0'), 38, 10) is null and 
                    src:sthe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sthe_ref_compnr), '0'), 38, 10) is null and 
                    src:sthe_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:txdt), '1900-01-01')) is null and 
                    src:txdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vers), '0'), 38, 10) is null and 
                    src:vers is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)