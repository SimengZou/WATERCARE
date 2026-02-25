CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINA124_ERROR AS SELECT src, 'LN_WHINA124' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dtwc), '0'), 38, 10) is null and 
                    src:amnt_dtwc is not null) THEN
                    'amnt_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_lclc), '0'), 38, 10) is null and 
                    src:amnt_lclc is not null) THEN
                    'amnt_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rfrc), '0'), 38, 10) is null and 
                    src:amnt_rfrc is not null) THEN
                    'amnt_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) THEN
                    'amnt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) THEN
                    'amnt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtf_1), '0'), 38, 10) is null and 
                    src:amtf_1 is not null) THEN
                    'amtf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amtf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtf_2), '0'), 38, 10) is null and 
                    src:amtf_2 is not null) THEN
                    'amtf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amtf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtf_3), '0'), 38, 10) is null and 
                    src:amtf_3 is not null) THEN
                    'amtf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amtf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt_1), '0'), 38, 10) is null and 
                    src:amtt_1 is not null) THEN
                    'amtt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amtt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt_2), '0'), 38, 10) is null and 
                    src:amtt_2 is not null) THEN
                    'amtt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amtt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt_3), '0'), 38, 10) is null and 
                    src:amtt_3 is not null) THEN
                    'amtt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amtt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atsf_ref_compnr), '0'), 38, 10) is null and 
                    src:atsf_ref_compnr is not null) THEN
                    'atsf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atsf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atst_ref_compnr), '0'), 38, 10) is null and 
                    src:atst_ref_compnr is not null) THEN
                    'atst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) THEN
                    'boml contains a non-numeric value : \'' || AS_VARCHAR(src:boml) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccpf_ref_compnr), '0'), 38, 10) is null and 
                    src:ccpf_ref_compnr is not null) THEN
                    'ccpf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccpf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccpt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccpt_ref_compnr is not null) THEN
                    'ccpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuso), '0'), 38, 10) is null and 
                    src:cuso is not null) THEN
                    'cuso contains a non-numeric value : \'' || AS_VARCHAR(src:cuso) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlin), '0'), 38, 10) is null and 
                    src:dlin is not null) THEN
                    'dlin contains a non-numeric value : \'' || AS_VARCHAR(src:dlin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expt), '0'), 38, 10) is null and 
                    src:expt is not null) THEN
                    'expt contains a non-numeric value : \'' || AS_VARCHAR(src:expt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fitr), '0'), 38, 10) is null and 
                    src:fitr is not null) THEN
                    'fitr contains a non-numeric value : \'' || AS_VARCHAR(src:fitr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:houf), '0'), 38, 10) is null and 
                    src:houf is not null) THEN
                    'houf contains a non-numeric value : \'' || AS_VARCHAR(src:houf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:houf_hour), '0'), 38, 10) is null and 
                    src:houf_hour is not null) THEN
                    'houf_hour contains a non-numeric value : \'' || AS_VARCHAR(src:houf_hour) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hout), '0'), 38, 10) is null and 
                    src:hout is not null) THEN
                    'hout contains a non-numeric value : \'' || AS_VARCHAR(src:hout) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itmf_ref_compnr), '0'), 38, 10) is null and 
                    src:itmf_ref_compnr is not null) THEN
                    'itmf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itmf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itmt_ref_compnr), '0'), 38, 10) is null and 
                    src:itmt_ref_compnr is not null) THEN
                    'itmt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itmt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itse), '0'), 38, 10) is null and 
                    src:itse is not null) THEN
                    'itse contains a non-numeric value : \'' || AS_VARCHAR(src:itse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivsq), '0'), 38, 10) is null and 
                    src:ivsq is not null) THEN
                    'ivsq contains a non-numeric value : \'' || AS_VARCHAR(src:ivsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) THEN
                    'koor contains a non-numeric value : \'' || AS_VARCHAR(src:koor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcln), '0'), 38, 10) is null and 
                    src:lcln is not null) THEN
                    'lcln contains a non-numeric value : \'' || AS_VARCHAR(src:lcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:logf), '0'), 38, 10) is null and 
                    src:logf is not null) THEN
                    'logf contains a non-numeric value : \'' || AS_VARCHAR(src:logf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:logt), '0'), 38, 10) is null and 
                    src:logt is not null) THEN
                    'logt contains a non-numeric value : \'' || AS_VARCHAR(src:logt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ocmp_ref_compnr is not null) THEN
                    'ocmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quaf), '0'), 38, 10) is null and 
                    src:quaf is not null) THEN
                    'quaf contains a non-numeric value : \'' || AS_VARCHAR(src:quaf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is null and 
                    src:quan_buar is not null) THEN
                    'quan_buar contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is null and 
                    src:quan_buln is not null) THEN
                    'quan_buln contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is null and 
                    src:quan_buvl is not null) THEN
                    'quan_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is null and 
                    src:quan_buwg is not null) THEN
                    'quan_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:quan_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_invu), '0'), 38, 10) is null and 
                    src:quan_invu is not null) THEN
                    'quan_invu contains a non-numeric value : \'' || AS_VARCHAR(src:quan_invu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quat), '0'), 38, 10) is null and 
                    src:quat is not null) THEN
                    'quat contains a non-numeric value : \'' || AS_VARCHAR(src:quat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) THEN
                    'rcln contains a non-numeric value : \'' || AS_VARCHAR(src:rcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) THEN
                    'rorg contains a non-numeric value : \'' || AS_VARCHAR(src:rorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tror), '0'), 38, 10) is null and 
                    src:tror is not null) THEN
                    'tror contains a non-numeric value : \'' || AS_VARCHAR(src:tror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typf), '0'), 38, 10) is null and 
                    src:typf is not null) THEN
                    'typf contains a non-numeric value : \'' || AS_VARCHAR(src:typf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typt), '0'), 38, 10) is null and 
                    src:typt is not null) THEN
                    'typt contains a non-numeric value : \'' || AS_VARCHAR(src:typt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unif_ref_compnr), '0'), 38, 10) is null and 
                    src:unif_ref_compnr is not null) THEN
                    'unif_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unif_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unit_ref_compnr), '0'), 38, 10) is null and 
                    src:unit_ref_compnr is not null) THEN
                    'unit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unit_ref_compnr) || '\'' WHEN 
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
                                    
                src:guid,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINA124 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dtwc), '0'), 38, 10) is null and 
                    src:amnt_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_lclc), '0'), 38, 10) is null and 
                    src:amnt_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rfrc), '0'), 38, 10) is null and 
                    src:amnt_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtf_1), '0'), 38, 10) is null and 
                    src:amtf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtf_2), '0'), 38, 10) is null and 
                    src:amtf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtf_3), '0'), 38, 10) is null and 
                    src:amtf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt_1), '0'), 38, 10) is null and 
                    src:amtt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt_2), '0'), 38, 10) is null and 
                    src:amtt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtt_3), '0'), 38, 10) is null and 
                    src:amtt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atsf_ref_compnr), '0'), 38, 10) is null and 
                    src:atsf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atst_ref_compnr), '0'), 38, 10) is null and 
                    src:atst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccpf_ref_compnr), '0'), 38, 10) is null and 
                    src:ccpf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccpt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuso), '0'), 38, 10) is null and 
                    src:cuso is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlin), '0'), 38, 10) is null and 
                    src:dlin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expt), '0'), 38, 10) is null and 
                    src:expt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fitr), '0'), 38, 10) is null and 
                    src:fitr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:houf), '0'), 38, 10) is null and 
                    src:houf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:houf_hour), '0'), 38, 10) is null and 
                    src:houf_hour is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hout), '0'), 38, 10) is null and 
                    src:hout is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itmf_ref_compnr), '0'), 38, 10) is null and 
                    src:itmf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itmt_ref_compnr), '0'), 38, 10) is null and 
                    src:itmt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itse), '0'), 38, 10) is null and 
                    src:itse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivsq), '0'), 38, 10) is null and 
                    src:ivsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcln), '0'), 38, 10) is null and 
                    src:lcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:logf), '0'), 38, 10) is null and 
                    src:logf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:logt), '0'), 38, 10) is null and 
                    src:logt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ocmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quaf), '0'), 38, 10) is null and 
                    src:quaf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is null and 
                    src:quan_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is null and 
                    src:quan_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is null and 
                    src:quan_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is null and 
                    src:quan_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan_invu), '0'), 38, 10) is null and 
                    src:quan_invu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quat), '0'), 38, 10) is null and 
                    src:quat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tror), '0'), 38, 10) is null and 
                    src:tror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typf), '0'), 38, 10) is null and 
                    src:typf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typt), '0'), 38, 10) is null and 
                    src:typt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unif_ref_compnr), '0'), 38, 10) is null and 
                    src:unif_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unit_ref_compnr), '0'), 38, 10) is null and 
                    src:unit_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)