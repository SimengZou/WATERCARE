CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDCMS050_ERROR AS SELECT src, 'LN_TDCMS050' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amic), '0'), 38, 10) is null and 
                    src:amic is not null) THEN
                    'amic contains a non-numeric value : \'' || AS_VARCHAR(src:amic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) THEN
                    'amoc contains a non-numeric value : \'' || AS_VARCHAR(src:amoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta), '0'), 38, 10) is null and 
                    src:amta is not null) THEN
                    'amta contains a non-numeric value : \'' || AS_VARCHAR(src:amta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_1), '0'), 38, 10) is null and 
                    src:cmam_1 is not null) THEN
                    'cmam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_2), '0'), 38, 10) is null and 
                    src:cmam_2 is not null) THEN
                    'cmam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_3), '0'), 38, 10) is null and 
                    src:cmam_3 is not null) THEN
                    'cmam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_dtwc), '0'), 38, 10) is null and 
                    src:cmam_dtwc is not null) THEN
                    'cmam_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_rfrc), '0'), 38, 10) is null and 
                    src:cmam_rfrc is not null) THEN
                    'cmam_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_rpc1), '0'), 38, 10) is null and 
                    src:cmam_rpc1 is not null) THEN
                    'cmam_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_rpc2), '0'), 38, 10) is null and 
                    src:cmam_rpc2 is not null) THEN
                    'cmam_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_trnc), '0'), 38, 10) is null and 
                    src:cmam_trnc is not null) THEN
                    'cmam_trnc contains a non-numeric value : \'' || AS_VARCHAR(src:cmam_trnc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) THEN
                    'cmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpr), '0'), 38, 10) is null and 
                    src:cmpr is not null) THEN
                    'cmpr contains a non-numeric value : \'' || AS_VARCHAR(src:cmpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmst), '0'), 38, 10) is null and 
                    src:cmst is not null) THEN
                    'cmst contains a non-numeric value : \'' || AS_VARCHAR(src:cmst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdt_ref_compnr), '0'), 38, 10) is null and 
                    src:cpdt_ref_compnr is not null) THEN
                    'cpdt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpdt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdt_year_perd_ref_compnr), '0'), 38, 10) is null and 
                    src:cpdt_year_perd_ref_compnr is not null) THEN
                    'cpdt_year_perd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpdt_year_perd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crin), '0'), 38, 10) is null and 
                    src:crin is not null) THEN
                    'crin contains a non-numeric value : \'' || AS_VARCHAR(src:crin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is null and 
                    src:crtp_ref_compnr is not null) THEN
                    'crtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsqn), '0'), 38, 10) is null and 
                    src:dsqn is not null) THEN
                    'dsqn contains a non-numeric value : \'' || AS_VARCHAR(src:dsqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) THEN
                    'fdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grpc), '0'), 38, 10) is null and 
                    src:grpc is not null) THEN
                    'grpc contains a non-numeric value : \'' || AS_VARCHAR(src:grpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icur_ref_compnr), '0'), 38, 10) is null and 
                    src:icur_ref_compnr is not null) THEN
                    'icur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:icur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invl), '0'), 38, 10) is null and 
                    src:invl is not null) THEN
                    'invl contains a non-numeric value : \'' || AS_VARCHAR(src:invl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) THEN
                    'invn contains a non-numeric value : \'' || AS_VARCHAR(src:invn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iraf_1), '0'), 38, 10) is null and 
                    src:iraf_1 is not null) THEN
                    'iraf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:iraf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iraf_2), '0'), 38, 10) is null and 
                    src:iraf_2 is not null) THEN
                    'iraf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:iraf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iraf_3), '0'), 38, 10) is null and 
                    src:iraf_3 is not null) THEN
                    'iraf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:iraf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irat_1), '0'), 38, 10) is null and 
                    src:irat_1 is not null) THEN
                    'irat_1 contains a non-numeric value : \'' || AS_VARCHAR(src:irat_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irat_2), '0'), 38, 10) is null and 
                    src:irat_2 is not null) THEN
                    'irat_2 contains a non-numeric value : \'' || AS_VARCHAR(src:irat_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irat_3), '0'), 38, 10) is null and 
                    src:irat_3 is not null) THEN
                    'irat_3 contains a non-numeric value : \'' || AS_VARCHAR(src:irat_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:irdt), '1900-01-01')) is null and 
                    src:irdt is not null) THEN
                    'irdt contains a non-timestamp value : \'' || AS_VARCHAR(src:irdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irtp_ref_compnr), '0'), 38, 10) is null and 
                    src:irtp_ref_compnr is not null) THEN
                    'irtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:irtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oraf_1), '0'), 38, 10) is null and 
                    src:oraf_1 is not null) THEN
                    'oraf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:oraf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oraf_2), '0'), 38, 10) is null and 
                    src:oraf_2 is not null) THEN
                    'oraf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:oraf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oraf_3), '0'), 38, 10) is null and 
                    src:oraf_3 is not null) THEN
                    'oraf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:oraf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orat_1), '0'), 38, 10) is null and 
                    src:orat_1 is not null) THEN
                    'orat_1 contains a non-numeric value : \'' || AS_VARCHAR(src:orat_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orat_2), '0'), 38, 10) is null and 
                    src:orat_2 is not null) THEN
                    'orat_2 contains a non-numeric value : \'' || AS_VARCHAR(src:orat_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orat_3), '0'), 38, 10) is null and 
                    src:orat_3 is not null) THEN
                    'orat_3 contains a non-numeric value : \'' || AS_VARCHAR(src:orat_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ordt), '1900-01-01')) is null and 
                    src:ordt is not null) THEN
                    'ordt contains a non-timestamp value : \'' || AS_VARCHAR(src:ordt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) THEN
                    'orno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp_ref_compnr), '0'), 38, 10) is null and 
                    src:ortp_ref_compnr is not null) THEN
                    'ortp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ortp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perd), '0'), 38, 10) is null and 
                    src:perd is not null) THEN
                    'perd contains a non-numeric value : \'' || AS_VARCHAR(src:perd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reld), '0'), 38, 10) is null and 
                    src:reld is not null) THEN
                    'reld contains a non-numeric value : \'' || AS_VARCHAR(src:reld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reln_ref_compnr), '0'), 38, 10) is null and 
                    src:reln_ref_compnr is not null) THEN
                    'reln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:resv), '0'), 38, 10) is null and 
                    src:resv is not null) THEN
                    'resv contains a non-numeric value : \'' || AS_VARCHAR(src:resv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlsq), '0'), 38, 10) is null and 
                    src:rlsq is not null) THEN
                    'rlsq contains a non-numeric value : \'' || AS_VARCHAR(src:rlsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:type), '0'), 38, 10) is null and 
                    src:type is not null) THEN
                    'type contains a non-numeric value : \'' || AS_VARCHAR(src:type) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upid), '0'), 38, 10) is null and 
                    src:upid is not null) THEN
                    'upid contains a non-numeric value : \'' || AS_VARCHAR(src:upid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
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
                src:orno,
                src:invl,
                src:pono,
                src:reln,
                src:type,
                src:sqnb,
                src:sern,
                src:dsqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDCMS050 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amic), '0'), 38, 10) is null and 
                    src:amic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amoc), '0'), 38, 10) is null and 
                    src:amoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amta), '0'), 38, 10) is null and 
                    src:amta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_1), '0'), 38, 10) is null and 
                    src:cmam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_2), '0'), 38, 10) is null and 
                    src:cmam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_3), '0'), 38, 10) is null and 
                    src:cmam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_dtwc), '0'), 38, 10) is null and 
                    src:cmam_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_rfrc), '0'), 38, 10) is null and 
                    src:cmam_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_rpc1), '0'), 38, 10) is null and 
                    src:cmam_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_rpc2), '0'), 38, 10) is null and 
                    src:cmam_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmam_trnc), '0'), 38, 10) is null and 
                    src:cmam_trnc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpr), '0'), 38, 10) is null and 
                    src:cmpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmst), '0'), 38, 10) is null and 
                    src:cmst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdt_ref_compnr), '0'), 38, 10) is null and 
                    src:cpdt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdt_year_perd_ref_compnr), '0'), 38, 10) is null and 
                    src:cpdt_year_perd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crin), '0'), 38, 10) is null and 
                    src:crin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is null and 
                    src:crtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsqn), '0'), 38, 10) is null and 
                    src:dsqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grpc), '0'), 38, 10) is null and 
                    src:grpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icur_ref_compnr), '0'), 38, 10) is null and 
                    src:icur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invl), '0'), 38, 10) is null and 
                    src:invl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invn), '0'), 38, 10) is null and 
                    src:invn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iraf_1), '0'), 38, 10) is null and 
                    src:iraf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iraf_2), '0'), 38, 10) is null and 
                    src:iraf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iraf_3), '0'), 38, 10) is null and 
                    src:iraf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irat_1), '0'), 38, 10) is null and 
                    src:irat_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irat_2), '0'), 38, 10) is null and 
                    src:irat_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irat_3), '0'), 38, 10) is null and 
                    src:irat_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:irdt), '1900-01-01')) is null and 
                    src:irdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:irtp_ref_compnr), '0'), 38, 10) is null and 
                    src:irtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oraf_1), '0'), 38, 10) is null and 
                    src:oraf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oraf_2), '0'), 38, 10) is null and 
                    src:oraf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oraf_3), '0'), 38, 10) is null and 
                    src:oraf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orat_1), '0'), 38, 10) is null and 
                    src:orat_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orat_2), '0'), 38, 10) is null and 
                    src:orat_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orat_3), '0'), 38, 10) is null and 
                    src:orat_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ordt), '1900-01-01')) is null and 
                    src:ordt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp_ref_compnr), '0'), 38, 10) is null and 
                    src:ortp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perd), '0'), 38, 10) is null and 
                    src:perd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reld), '0'), 38, 10) is null and 
                    src:reld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reln_ref_compnr), '0'), 38, 10) is null and 
                    src:reln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:resv), '0'), 38, 10) is null and 
                    src:resv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlsq), '0'), 38, 10) is null and 
                    src:rlsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:type), '0'), 38, 10) is null and 
                    src:type is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upid), '0'), 38, 10) is null and 
                    src:upid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)