CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSSOC205_ERROR AS SELECT src, 'LN_TSSOC205' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acdu), '0'), 38, 10) is null and 
                    src:acdu is not null) THEN
                    'acdu contains a non-numeric value : \'' || AS_VARCHAR(src:acdu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) THEN
                    'acln contains a non-numeric value : \'' || AS_VARCHAR(src:acln) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:actm), '1900-01-01')) is null and 
                    src:actm is not null) THEN
                    'actm contains a non-timestamp value : \'' || AS_VARCHAR(src:actm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actp), '0'), 38, 10) is null and 
                    src:actp is not null) THEN
                    'actp contains a non-numeric value : \'' || AS_VARCHAR(src:actp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aftm), '1900-01-01')) is null and 
                    src:aftm is not null) THEN
                    'aftm contains a non-timestamp value : \'' || AS_VARCHAR(src:aftm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:astm), '1900-01-01')) is null and 
                    src:astm is not null) THEN
                    'astm contains a non-timestamp value : \'' || AS_VARCHAR(src:astm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atft), '1900-01-01')) is null and 
                    src:atft is not null) THEN
                    'atft contains a non-timestamp value : \'' || AS_VARCHAR(src:atft) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atst), '1900-01-01')) is null and 
                    src:atst is not null) THEN
                    'atst contains a non-timestamp value : \'' || AS_VARCHAR(src:atst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmbb_orno_acln_ref_compnr), '0'), 38, 10) is null and 
                    src:cmbb_orno_acln_ref_compnr is not null) THEN
                    'cmbb_orno_acln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmbb_orno_acln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmbc_orno_acln_ref_compnr), '0'), 38, 10) is null and 
                    src:cmbc_orno_acln_ref_compnr is not null) THEN
                    'cmbc_orno_acln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmbc_orno_acln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dltm), '1900-01-01')) is null and 
                    src:dltm is not null) THEN
                    'dltm contains a non-timestamp value : \'' || AS_VARCHAR(src:dltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) THEN
                    'emno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:emno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:esdt), '1900-01-01')) is null and 
                    src:esdt is not null) THEN
                    'esdt contains a non-timestamp value : \'' || AS_VARCHAR(src:esdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:jctm), '1900-01-01')) is null and 
                    src:jctm is not null) THEN
                    'jctm contains a non-timestamp value : \'' || AS_VARCHAR(src:jctm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:line), '0'), 38, 10) is null and 
                    src:line is not null) THEN
                    'line contains a non-numeric value : \'' || AS_VARCHAR(src:line) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltct), '1900-01-01')) is null and 
                    src:ltct is not null) THEN
                    'ltct contains a non-timestamp value : \'' || AS_VARCHAR(src:ltct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orgn), '0'), 38, 10) is null and 
                    src:orgn is not null) THEN
                    'orgn contains a non-numeric value : \'' || AS_VARCHAR(src:orgn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pftm), '1900-01-01')) is null and 
                    src:pftm is not null) THEN
                    'pftm contains a non-timestamp value : \'' || AS_VARCHAR(src:pftm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgcn), '0'), 38, 10) is null and 
                    src:pgcn is not null) THEN
                    'pgcn contains a non-numeric value : \'' || AS_VARCHAR(src:pgcn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pgdt), '1900-01-01')) is null and 
                    src:pgdt is not null) THEN
                    'pgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:pgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pged), '0'), 38, 10) is null and 
                    src:pged is not null) THEN
                    'pged contains a non-numeric value : \'' || AS_VARCHAR(src:pged) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgit), '0'), 38, 10) is null and 
                    src:pgit is not null) THEN
                    'pgit contains a non-numeric value : \'' || AS_VARCHAR(src:pgit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgrd), '0'), 38, 10) is null and 
                    src:pgrd is not null) THEN
                    'pgrd contains a non-numeric value : \'' || AS_VARCHAR(src:pgrd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pstm), '1900-01-01')) is null and 
                    src:pstm is not null) THEN
                    'pstm contains a non-timestamp value : \'' || AS_VARCHAR(src:pstm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptft), '1900-01-01')) is null and 
                    src:ptft is not null) THEN
                    'ptft contains a non-timestamp value : \'' || AS_VARCHAR(src:ptft) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptst), '1900-01-01')) is null and 
                    src:ptst is not null) THEN
                    'ptst contains a non-timestamp value : \'' || AS_VARCHAR(src:ptst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rejr_ref_compnr), '0'), 38, 10) is null and 
                    src:rejr_ref_compnr is not null) THEN
                    'rejr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rejr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rjtm), '1900-01-01')) is null and 
                    src:rjtm is not null) THEN
                    'rjtm contains a non-timestamp value : \'' || AS_VARCHAR(src:rjtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi), '0'), 38, 10) is null and 
                    src:trdi is not null) THEN
                    'trdi contains a non-numeric value : \'' || AS_VARCHAR(src:trdi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi_buln), '0'), 38, 10) is null and 
                    src:trdi_buln is not null) THEN
                    'trdi_buln contains a non-numeric value : \'' || AS_VARCHAR(src:trdi_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdu), '0'), 38, 10) is null and 
                    src:trdu is not null) THEN
                    'trdu contains a non-numeric value : \'' || AS_VARCHAR(src:trdu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdu_butm), '0'), 38, 10) is null and 
                    src:trdu_butm is not null) THEN
                    'trdu_butm contains a non-numeric value : \'' || AS_VARCHAR(src:trdu_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uecp), '0'), 38, 10) is null and 
                    src:uecp is not null) THEN
                    'uecp contains a non-numeric value : \'' || AS_VARCHAR(src:uecp) || '\'' WHEN 
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
                                    
                src:orgn,
                src:compnr,
                src:line,
                src:orno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSSOC205 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acdu), '0'), 38, 10) is null and 
                    src:acdu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:actm), '1900-01-01')) is null and 
                    src:actm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actp), '0'), 38, 10) is null and 
                    src:actp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:aftm), '1900-01-01')) is null and 
                    src:aftm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:astm), '1900-01-01')) is null and 
                    src:astm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atft), '1900-01-01')) is null and 
                    src:atft is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:atst), '1900-01-01')) is null and 
                    src:atst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmbb_orno_acln_ref_compnr), '0'), 38, 10) is null and 
                    src:cmbb_orno_acln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmbc_orno_acln_ref_compnr), '0'), 38, 10) is null and 
                    src:cmbc_orno_acln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dltm), '1900-01-01')) is null and 
                    src:dltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is null and 
                    src:emno_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:esdt), '1900-01-01')) is null and 
                    src:esdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:jctm), '1900-01-01')) is null and 
                    src:jctm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:line), '0'), 38, 10) is null and 
                    src:line is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltct), '1900-01-01')) is null and 
                    src:ltct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orgn), '0'), 38, 10) is null and 
                    src:orgn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pftm), '1900-01-01')) is null and 
                    src:pftm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgcn), '0'), 38, 10) is null and 
                    src:pgcn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pgdt), '1900-01-01')) is null and 
                    src:pgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pged), '0'), 38, 10) is null and 
                    src:pged is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgit), '0'), 38, 10) is null and 
                    src:pgit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgrd), '0'), 38, 10) is null and 
                    src:pgrd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pstm), '1900-01-01')) is null and 
                    src:pstm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptft), '1900-01-01')) is null and 
                    src:ptft is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ptst), '1900-01-01')) is null and 
                    src:ptst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rejr_ref_compnr), '0'), 38, 10) is null and 
                    src:rejr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rjtm), '1900-01-01')) is null and 
                    src:rjtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi), '0'), 38, 10) is null and 
                    src:trdi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdi_buln), '0'), 38, 10) is null and 
                    src:trdi_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdu), '0'), 38, 10) is null and 
                    src:trdu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdu_butm), '0'), 38, 10) is null and 
                    src:trdu_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uecp), '0'), 38, 10) is null and 
                    src:uecp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)