CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSMI110_ERROR AS SELECT src, 'LN_TDSMI110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) THEN
                    'acdt contains a non-timestamp value : \'' || AS_VARCHAR(src:acdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:catt_ref_compnr), '0'), 38, 10) is null and 
                    src:catt_ref_compnr is not null) THEN
                    'catt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:catt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccor_ref_compnr), '0'), 38, 10) is null and 
                    src:ccor_ref_compnr is not null) THEN
                    'ccor_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccor_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) THEN
                    'cdis_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdis_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpha_ref_compnr), '0'), 38, 10) is null and 
                    src:cpha_ref_compnr is not null) THEN
                    'cpha_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpha_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptp_ref_compnr), '0'), 38, 10) is null and 
                    src:cptp_ref_compnr is not null) THEN
                    'cptp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cptp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csou_ref_compnr), '0'), 38, 10) is null and 
                    src:csou_ref_compnr is not null) THEN
                    'csou_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csou_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) THEN
                    'dcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:dcdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fcdt), '1900-01-01')) is null and 
                    src:fcdt is not null) THEN
                    'fcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:fcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incf), '0'), 38, 10) is null and 
                    src:incf is not null) THEN
                    'incf contains a non-numeric value : \'' || AS_VARCHAR(src:incf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) THEN
                    'ltdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ltdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opst), '0'), 38, 10) is null and 
                    src:opst is not null) THEN
                    'opst contains a non-numeric value : \'' || AS_VARCHAR(src:opst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:role), '0'), 38, 10) is null and 
                    src:role is not null) THEN
                    'role contains a non-numeric value : \'' || AS_VARCHAR(src:role) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sapr_cpha_ref_compnr), '0'), 38, 10) is null and 
                    src:sapr_cpha_ref_compnr is not null) THEN
                    'sapr_cpha_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sapr_cpha_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sapr_ref_compnr), '0'), 38, 10) is null and 
                    src:sapr_ref_compnr is not null) THEN
                    'sapr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sapr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccp), '0'), 38, 10) is null and 
                    src:sccp is not null) THEN
                    'sccp contains a non-numeric value : \'' || AS_VARCHAR(src:sccp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam), '0'), 38, 10) is null and 
                    src:xpam is not null) THEN
                    'xpam contains a non-numeric value : \'' || AS_VARCHAR(src:xpam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_dtwc), '0'), 38, 10) is null and 
                    src:xpam_dtwc is not null) THEN
                    'xpam_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:xpam_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_lclc), '0'), 38, 10) is null and 
                    src:xpam_lclc is not null) THEN
                    'xpam_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:xpam_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_rfrc), '0'), 38, 10) is null and 
                    src:xpam_rfrc is not null) THEN
                    'xpam_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:xpam_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_rpc1), '0'), 38, 10) is null and 
                    src:xpam_rpc1 is not null) THEN
                    'xpam_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:xpam_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_rpc2), '0'), 38, 10) is null and 
                    src:xpam_rpc2 is not null) THEN
                    'xpam_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:xpam_rpc2) || '\'' WHEN 
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
                src:opty  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSMI110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:catt_ref_compnr), '0'), 38, 10) is null and 
                    src:catt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccor_ref_compnr), '0'), 38, 10) is null and 
                    src:ccor_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis_ref_compnr), '0'), 38, 10) is null and 
                    src:cdis_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpha_ref_compnr), '0'), 38, 10) is null and 
                    src:cpha_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cptp_ref_compnr), '0'), 38, 10) is null and 
                    src:cptp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csou_ref_compnr), '0'), 38, 10) is null and 
                    src:csou_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fcdt), '1900-01-01')) is null and 
                    src:fcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incf), '0'), 38, 10) is null and 
                    src:incf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opst), '0'), 38, 10) is null and 
                    src:opst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:role), '0'), 38, 10) is null and 
                    src:role is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sapr_cpha_ref_compnr), '0'), 38, 10) is null and 
                    src:sapr_cpha_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sapr_ref_compnr), '0'), 38, 10) is null and 
                    src:sapr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccp), '0'), 38, 10) is null and 
                    src:sccp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam), '0'), 38, 10) is null and 
                    src:xpam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_dtwc), '0'), 38, 10) is null and 
                    src:xpam_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_lclc), '0'), 38, 10) is null and 
                    src:xpam_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_rfrc), '0'), 38, 10) is null and 
                    src:xpam_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_rpc1), '0'), 38, 10) is null and 
                    src:xpam_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xpam_rpc2), '0'), 38, 10) is null and 
                    src:xpam_rpc2 is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)