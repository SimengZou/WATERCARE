CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM121_ERROR AS SELECT src, 'LN_TCCOM121' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) THEN
                    'qual contains a non-numeric value : \'' || AS_VARCHAR(src:qual) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is null and 
                    src:rdec_ref_compnr is not null) THEN
                    'rdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sasn), '0'), 38, 10) is null and 
                    src:sasn is not null) THEN
                    'sasn contains a non-numeric value : \'' || AS_VARCHAR(src:sasn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) THEN
                    'serv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:serv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                src:sfbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM121 as strm)
                WHERE
                ROWNUMBER=1) where 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is null and 
                    src:rdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sasn), '0'), 38, 10) is null and 
                    src:sasn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)