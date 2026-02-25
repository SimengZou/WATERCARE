CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TIPCF500_ERROR AS SELECT src, 'LN_TIPCF500' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfs), '0'), 38, 10) is null and 
                    src:acfs is not null) THEN
                    'acfs contains a non-numeric value : \'' || AS_VARCHAR(src:acfs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfv), '0'), 38, 10) is null and 
                    src:acfv is not null) THEN
                    'acfv contains a non-numeric value : \'' || AS_VARCHAR(src:acfv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altn), '0'), 38, 10) is null and 
                    src:altn is not null) THEN
                    'altn contains a non-numeric value : \'' || AS_VARCHAR(src:altn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) THEN
                    'cpva contains a non-numeric value : \'' || AS_VARCHAR(src:cpva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuno_ref_compnr), '0'), 38, 10) is null and 
                    src:cuno_ref_compnr is not null) THEN
                    'cuno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enho), '0'), 38, 10) is null and 
                    src:enho is not null) THEN
                    'enho contains a non-numeric value : \'' || AS_VARCHAR(src:enho) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irft), '0'), 38, 10) is null and 
                    src:irft is not null) THEN
                    'irft contains a non-numeric value : \'' || AS_VARCHAR(src:irft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnst), '0'), 38, 10) is null and 
                    src:lnst is not null) THEN
                    'lnst contains a non-numeric value : \'' || AS_VARCHAR(src:lnst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pcfd), '1900-01-01')) is null and 
                    src:pcfd is not null) THEN
                    'pcfd contains a non-timestamp value : \'' || AS_VARCHAR(src:pcfd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qana), '0'), 38, 10) is null and 
                    src:qana is not null) THEN
                    'qana contains a non-numeric value : \'' || AS_VARCHAR(src:qana) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:refp), '0'), 38, 10) is null and 
                    src:refp is not null) THEN
                    'refp contains a non-numeric value : \'' || AS_VARCHAR(src:refp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reft), '0'), 38, 10) is null and 
                    src:reft is not null) THEN
                    'reft contains a non-numeric value : \'' || AS_VARCHAR(src:reft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slpr), '0'), 38, 10) is null and 
                    src:slpr is not null) THEN
                    'slpr contains a non-numeric value : \'' || AS_VARCHAR(src:slpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sost), '0'), 38, 10) is null and 
                    src:sost is not null) THEN
                    'sost contains a non-numeric value : \'' || AS_VARCHAR(src:sost) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spcf), '0'), 38, 10) is null and 
                    src:spcf is not null) THEN
                    'spcf contains a non-numeric value : \'' || AS_VARCHAR(src:spcf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vali), '0'), 38, 10) is null and 
                    src:vali is not null) THEN
                    'vali contains a non-numeric value : \'' || AS_VARCHAR(src:vali) || '\'' WHEN 
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
                src:cpva  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIPCF500 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfs), '0'), 38, 10) is null and 
                    src:acfs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfv), '0'), 38, 10) is null and 
                    src:acfv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altn), '0'), 38, 10) is null and 
                    src:altn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpva), '0'), 38, 10) is null and 
                    src:cpva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuno_ref_compnr), '0'), 38, 10) is null and 
                    src:cuno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enho), '0'), 38, 10) is null and 
                    src:enho is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irft), '0'), 38, 10) is null and 
                    src:irft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnst), '0'), 38, 10) is null and 
                    src:lnst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pcfd), '1900-01-01')) is null and 
                    src:pcfd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qana), '0'), 38, 10) is null and 
                    src:qana is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:refp), '0'), 38, 10) is null and 
                    src:refp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reft), '0'), 38, 10) is null and 
                    src:reft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slpr), '0'), 38, 10) is null and 
                    src:slpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sost), '0'), 38, 10) is null and 
                    src:sost is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spcf), '0'), 38, 10) is null and 
                    src:spcf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vali), '0'), 38, 10) is null and 
                    src:vali is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)