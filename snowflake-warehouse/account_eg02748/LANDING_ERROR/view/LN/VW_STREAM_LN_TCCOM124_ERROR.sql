CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM124_ERROR AS SELECT src, 'LN_TCCOM124' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aofc_ref_compnr), '0'), 38, 10) is null and 
                    src:aofc_ref_compnr is not null) THEN
                    'aofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:aofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcbs), '0'), 38, 10) is null and 
                    src:bcbs is not null) THEN
                    'bcbs contains a non-numeric value : \'' || AS_VARCHAR(src:bcbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) THEN
                    'bpst contains a non-numeric value : \'' || AS_VARCHAR(src:bpst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) THEN
                    'cbps_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbps_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) THEN
                    'cbtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) THEN
                    'ccnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsm), '0'), 38, 10) is null and 
                    src:cnsm is not null) THEN
                    'cnsm contains a non-numeric value : \'' || AS_VARCHAR(src:cnsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eded), '0'), 38, 10) is null and 
                    src:eded is not null) THEN
                    'eded contains a non-numeric value : \'' || AS_VARCHAR(src:eded) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exor), '0'), 38, 10) is null and 
                    src:exor is not null) THEN
                    'exor contains a non-numeric value : \'' || AS_VARCHAR(src:exor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expo), '0'), 38, 10) is null and 
                    src:expo is not null) THEN
                    'expo contains a non-numeric value : \'' || AS_VARCHAR(src:expo) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mxtn), '0'), 38, 10) is null and 
                    src:mxtn is not null) THEN
                    'mxtn contains a non-numeric value : \'' || AS_VARCHAR(src:mxtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_cban_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_cban_ref_compnr is not null) THEN
                    'ptbp_cban_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_cban_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stxa), '0'), 38, 10) is null and 
                    src:stxa is not null) THEN
                    'stxa contains a non-numeric value : \'' || AS_VARCHAR(src:stxa) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tndm), '0'), 38, 10) is null and 
                    src:tndm is not null) THEN
                    'tndm contains a non-numeric value : \'' || AS_VARCHAR(src:tndm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uptc), '0'), 38, 10) is null and 
                    src:uptc is not null) THEN
                    'uptc contains a non-numeric value : \'' || AS_VARCHAR(src:uptc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwht), '0'), 38, 10) is null and 
                    src:uwht is not null) THEN
                    'uwht contains a non-numeric value : \'' || AS_VARCHAR(src:uwht) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:valf), '1900-01-01')) is null and 
                    src:valf is not null) THEN
                    'valf contains a non-timestamp value : \'' || AS_VARCHAR(src:valf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:valt), '1900-01-01')) is null and 
                    src:valt is not null) THEN
                    'valt contains a non-timestamp value : \'' || AS_VARCHAR(src:valt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) THEN
                    'vryn contains a non-numeric value : \'' || AS_VARCHAR(src:vryn) || '\'' WHEN 
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
                src:ptbp,
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM124 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aofc_ref_compnr), '0'), 38, 10) is null and 
                    src:aofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcbs), '0'), 38, 10) is null and 
                    src:bcbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsm), '0'), 38, 10) is null and 
                    src:cnsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eded), '0'), 38, 10) is null and 
                    src:eded is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exor), '0'), 38, 10) is null and 
                    src:exor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expo), '0'), 38, 10) is null and 
                    src:expo is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mxtn), '0'), 38, 10) is null and 
                    src:mxtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_cban_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_cban_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stxa), '0'), 38, 10) is null and 
                    src:stxa is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tndm), '0'), 38, 10) is null and 
                    src:tndm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uptc), '0'), 38, 10) is null and 
                    src:uptc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwht), '0'), 38, 10) is null and 
                    src:uwht is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:valf), '1900-01-01')) is null and 
                    src:valf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:valt), '1900-01-01')) is null and 
                    src:valt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)