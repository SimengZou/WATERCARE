CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSMI113_ERROR AS SELECT src, 'LN_TDSMI113' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta), '0'), 38, 10) is null and 
                    src:amta is not null) THEN
                    'amta contains a non-numeric value : \'' || AS_VARCHAR(src:amta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_dtwc), '0'), 38, 10) is null and 
                    src:amta_dtwc is not null) THEN
                    'amta_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amta_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_lclc), '0'), 38, 10) is null and 
                    src:amta_lclc is not null) THEN
                    'amta_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amta_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_rfrc), '0'), 38, 10) is null and 
                    src:amta_rfrc is not null) THEN
                    'amta_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amta_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_rpc1), '0'), 38, 10) is null and 
                    src:amta_rpc1 is not null) THEN
                    'amta_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amta_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_rpc2), '0'), 38, 10) is null and 
                    src:amta_rpc2 is not null) THEN
                    'amta_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amta_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_dtwc), '0'), 38, 10) is null and 
                    src:amtg_dtwc is not null) THEN
                    'amtg_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amtg_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_lclc), '0'), 38, 10) is null and 
                    src:amtg_lclc is not null) THEN
                    'amtg_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amtg_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_rfrc), '0'), 38, 10) is null and 
                    src:amtg_rfrc is not null) THEN
                    'amtg_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amtg_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_rpc1), '0'), 38, 10) is null and 
                    src:amtg_rpc1 is not null) THEN
                    'amtg_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amtg_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_rpc2), '0'), 38, 10) is null and 
                    src:amtg_rpc2 is not null) THEN
                    'amtg_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amtg_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_trnc), '0'), 38, 10) is null and 
                    src:amtg_trnc is not null) THEN
                    'amtg_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:amtg_trnc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) THEN
                    'cuqs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) THEN
                    'cvqs contains a non-numeric value : \'' || AS_VARCHAR(src:cvqs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opty_ref_compnr), '0'), 38, 10) is null and 
                    src:opty_ref_compnr is not null) THEN
                    'opty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:opty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) THEN
                    'qoor contains a non-numeric value : \'' || AS_VARCHAR(src:qoor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buar), '0'), 38, 10) is null and 
                    src:qoor_buar is not null) THEN
                    'qoor_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buln), '0'), 38, 10) is null and 
                    src:qoor_buln is not null) THEN
                    'qoor_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_bupc), '0'), 38, 10) is null and 
                    src:qoor_bupc is not null) THEN
                    'qoor_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_butm), '0'), 38, 10) is null and 
                    src:qoor_butm is not null) THEN
                    'qoor_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buvl), '0'), 38, 10) is null and 
                    src:qoor_buvl is not null) THEN
                    'qoor_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buwg), '0'), 38, 10) is null and 
                    src:qoor_buwg is not null) THEN
                    'qoor_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_invu), '0'), 38, 10) is null and 
                    src:qoor_invu is not null) THEN
                    'qoor_invu contains a non-numeric value : \'' || AS_VARCHAR(src:qoor_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seli), '0'), 38, 10) is null and 
                    src:seli is not null) THEN
                    'seli contains a non-numeric value : \'' || AS_VARCHAR(src:seli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
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
                                    
                src:opty,
                src:pono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSMI113 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta), '0'), 38, 10) is null and 
                    src:amta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_dtwc), '0'), 38, 10) is null and 
                    src:amta_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_lclc), '0'), 38, 10) is null and 
                    src:amta_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_rfrc), '0'), 38, 10) is null and 
                    src:amta_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_rpc1), '0'), 38, 10) is null and 
                    src:amta_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta_rpc2), '0'), 38, 10) is null and 
                    src:amta_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_dtwc), '0'), 38, 10) is null and 
                    src:amtg_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_lclc), '0'), 38, 10) is null and 
                    src:amtg_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_rfrc), '0'), 38, 10) is null and 
                    src:amtg_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_rpc1), '0'), 38, 10) is null and 
                    src:amtg_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_rpc2), '0'), 38, 10) is null and 
                    src:amtg_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtg_trnc), '0'), 38, 10) is null and 
                    src:amtg_trnc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvqs), '0'), 38, 10) is null and 
                    src:cvqs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opty_ref_compnr), '0'), 38, 10) is null and 
                    src:opty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buar), '0'), 38, 10) is null and 
                    src:qoor_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buln), '0'), 38, 10) is null and 
                    src:qoor_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_bupc), '0'), 38, 10) is null and 
                    src:qoor_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_butm), '0'), 38, 10) is null and 
                    src:qoor_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buvl), '0'), 38, 10) is null and 
                    src:qoor_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_buwg), '0'), 38, 10) is null and 
                    src:qoor_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor_invu), '0'), 38, 10) is null and 
                    src:qoor_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seli), '0'), 38, 10) is null and 
                    src:seli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)