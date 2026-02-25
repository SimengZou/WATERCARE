CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH440_ERROR AS SELECT src, 'LN_WHINH440' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aetc), '0'), 38, 10) is null and 
                    src:aetc is not null) THEN
                    'aetc contains a non-numeric value : \'' || AS_VARCHAR(src:aetc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aetr), '0'), 38, 10) is null and 
                    src:aetr is not null) THEN
                    'aetr contains a non-numeric value : \'' || AS_VARCHAR(src:aetr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:card_ref_compnr), '0'), 38, 10) is null and 
                    src:card_ref_compnr is not null) THEN
                    'card_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:card_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carp_ref_compnr), '0'), 38, 10) is null and 
                    src:carp_ref_compnr is not null) THEN
                    'carp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctnt), '0'), 38, 10) is null and 
                    src:ctnt is not null) THEN
                    'ctnt contains a non-numeric value : \'' || AS_VARCHAR(src:ctnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eacu_ref_compnr), '0'), 38, 10) is null and 
                    src:eacu_ref_compnr is not null) THEN
                    'eacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:eacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt), '0'), 38, 10) is null and 
                    src:eamt is not null) THEN
                    'eamt contains a non-numeric value : \'' || AS_VARCHAR(src:eamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etrn_ref_compnr), '0'), 38, 10) is null and 
                    src:etrn_ref_compnr is not null) THEN
                    'etrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:etrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxwt), '0'), 38, 10) is null and 
                    src:fxwt is not null) THEN
                    'fxwt contains a non-numeric value : \'' || AS_VARCHAR(src:fxwt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mrdt), '1900-01-01')) is null and 
                    src:mrdt is not null) THEN
                    'mrdt contains a non-timestamp value : \'' || AS_VARCHAR(src:mrdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrtp), '0'), 38, 10) is null and 
                    src:mrtp is not null) THEN
                    'mrtp contains a non-numeric value : \'' || AS_VARCHAR(src:mrtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mwgt), '0'), 38, 10) is null and 
                    src:mwgt is not null) THEN
                    'mwgt contains a non-numeric value : \'' || AS_VARCHAR(src:mwgt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) THEN
                    'pddt contains a non-timestamp value : \'' || AS_VARCHAR(src:pddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) THEN
                    'psts contains a non-numeric value : \'' || AS_VARCHAR(src:psts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptro), '0'), 38, 10) is null and 
                    src:ptro is not null) THEN
                    'ptro contains a non-numeric value : \'' || AS_VARCHAR(src:ptro) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sans), '0'), 38, 10) is null and 
                    src:sans is not null) THEN
                    'sans contains a non-numeric value : \'' || AS_VARCHAR(src:sans) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shst), '0'), 38, 10) is null and 
                    src:shst is not null) THEN
                    'shst contains a non-numeric value : \'' || AS_VARCHAR(src:shst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdl), '0'), 38, 10) is null and 
                    src:sqdl is not null) THEN
                    'sqdl contains a non-numeric value : \'' || AS_VARCHAR(src:sqdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdp), '0'), 38, 10) is null and 
                    src:sqdp is not null) THEN
                    'sqdp contains a non-numeric value : \'' || AS_VARCHAR(src:sqdp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sspl), '0'), 38, 10) is null and 
                    src:sspl is not null) THEN
                    'sspl contains a non-numeric value : \'' || AS_VARCHAR(src:sspl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) THEN
                    'stcp contains a non-numeric value : \'' || AS_VARCHAR(src:stcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) THEN
                    'stty contains a non-numeric value : \'' || AS_VARCHAR(src:stty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcap), '0'), 38, 10) is null and 
                    src:tcap is not null) THEN
                    'tcap contains a non-numeric value : \'' || AS_VARCHAR(src:tcap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tccp), '0'), 38, 10) is null and 
                    src:tccp is not null) THEN
                    'tccp contains a non-numeric value : \'' || AS_VARCHAR(src:tccp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tnta), '0'), 38, 10) is null and 
                    src:tnta is not null) THEN
                    'tnta contains a non-numeric value : \'' || AS_VARCHAR(src:tnta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tntb), '0'), 38, 10) is null and 
                    src:tntb is not null) THEN
                    'tntb contains a non-numeric value : \'' || AS_VARCHAR(src:tntb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tntp), '0'), 38, 10) is null and 
                    src:tntp is not null) THEN
                    'tntp contains a non-numeric value : \'' || AS_VARCHAR(src:tntp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:todd), '1900-01-01')) is null and 
                    src:todd is not null) THEN
                    'todd contains a non-timestamp value : \'' || AS_VARCHAR(src:todd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tolt), '0'), 38, 10) is null and 
                    src:tolt is not null) THEN
                    'tolt contains a non-numeric value : \'' || AS_VARCHAR(src:tolt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tops), '0'), 38, 10) is null and 
                    src:tops is not null) THEN
                    'tops contains a non-numeric value : \'' || AS_VARCHAR(src:tops) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:totu), '0'), 38, 10) is null and 
                    src:totu is not null) THEN
                    'totu contains a non-numeric value : \'' || AS_VARCHAR(src:totu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trmp), '0'), 38, 10) is null and 
                    src:trmp is not null) THEN
                    'trmp contains a non-numeric value : \'' || AS_VARCHAR(src:trmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
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
                src:load  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH440 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aetc), '0'), 38, 10) is null and 
                    src:aetc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aetr), '0'), 38, 10) is null and 
                    src:aetr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:card_ref_compnr), '0'), 38, 10) is null and 
                    src:card_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carp_ref_compnr), '0'), 38, 10) is null and 
                    src:carp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctnt), '0'), 38, 10) is null and 
                    src:ctnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eacu_ref_compnr), '0'), 38, 10) is null and 
                    src:eacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eamt), '0'), 38, 10) is null and 
                    src:eamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etrn_ref_compnr), '0'), 38, 10) is null and 
                    src:etrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxwt), '0'), 38, 10) is null and 
                    src:fxwt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mrdt), '1900-01-01')) is null and 
                    src:mrdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrtp), '0'), 38, 10) is null and 
                    src:mrtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mwgt), '0'), 38, 10) is null and 
                    src:mwgt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptro), '0'), 38, 10) is null and 
                    src:ptro is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sans), '0'), 38, 10) is null and 
                    src:sans is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shst), '0'), 38, 10) is null and 
                    src:shst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdl), '0'), 38, 10) is null and 
                    src:sqdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqdp), '0'), 38, 10) is null and 
                    src:sqdp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sspl), '0'), 38, 10) is null and 
                    src:sspl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcap), '0'), 38, 10) is null and 
                    src:tcap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tccp), '0'), 38, 10) is null and 
                    src:tccp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tnta), '0'), 38, 10) is null and 
                    src:tnta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tntb), '0'), 38, 10) is null and 
                    src:tntb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tntp), '0'), 38, 10) is null and 
                    src:tntp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:todd), '1900-01-01')) is null and 
                    src:todd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tolt), '0'), 38, 10) is null and 
                    src:tolt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tops), '0'), 38, 10) is null and 
                    src:tops is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:totu), '0'), 38, 10) is null and 
                    src:totu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trmp), '0'), 38, 10) is null and 
                    src:trmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)