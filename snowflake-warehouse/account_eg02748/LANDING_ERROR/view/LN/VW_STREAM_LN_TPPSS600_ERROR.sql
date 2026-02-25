CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPSS600_ERROR AS SELECT src, 'LN_TPPSS600' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aitm_ref_compnr), '0'), 38, 10) is null and 
                    src:aitm_ref_compnr is not null) THEN
                    'aitm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:aitm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bsqn), '0'), 38, 10) is null and 
                    src:bsqn is not null) THEN
                    'bsqn contains a non-numeric value : \'' || AS_VARCHAR(src:bsqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buyr_ref_compnr), '0'), 38, 10) is null and 
                    src:buyr_ref_compnr is not null) THEN
                    'buyr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:buyr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) THEN
                    'carr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) THEN
                    'cprj_cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) THEN
                    'crbn contains a non-numeric value : \'' || AS_VARCHAR(src:crbn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:date), '1900-01-01')) is null and 
                    src:date is not null) THEN
                    'date contains a non-timestamp value : \'' || AS_VARCHAR(src:date) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:deli), '0'), 38, 10) is null and 
                    src:deli is not null) THEN
                    'deli contains a non-numeric value : \'' || AS_VARCHAR(src:deli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsit_ref_compnr), '0'), 38, 10) is null and 
                    src:dsit_ref_compnr is not null) THEN
                    'dsit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dsit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) THEN
                    'dwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effw), '0'), 38, 10) is null and 
                    src:effw is not null) THEN
                    'effw contains a non-numeric value : \'' || AS_VARCHAR(src:effw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effw_ref_compnr), '0'), 38, 10) is null and 
                    src:effw_ref_compnr is not null) THEN
                    'effw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph), '0'), 38, 10) is null and 
                    src:efph is not null) THEN
                    'efph contains a non-numeric value : \'' || AS_VARCHAR(src:efph) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph_ref_compnr), '0'), 38, 10) is null and 
                    src:efph_ref_compnr is not null) THEN
                    'efph_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:efph_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) THEN
                    'koor contains a non-numeric value : \'' || AS_VARCHAR(src:koor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kotr), '0'), 38, 10) is null and 
                    src:kotr is not null) THEN
                    'kotr contains a non-numeric value : \'' || AS_VARCHAR(src:kotr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phit_ref_compnr), '0'), 38, 10) is null and 
                    src:phit_ref_compnr is not null) THEN
                    'phit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:phit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is null and 
                    src:plid_ref_compnr is not null) THEN
                    'plid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) THEN
                    'quan contains a non-numeric value : \'' || AS_VARCHAR(src:quan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtdl), '0'), 38, 10) is null and 
                    src:rtdl is not null) THEN
                    'rtdl contains a non-numeric value : \'' || AS_VARCHAR(src:rtdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) THEN
                    'serv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:serv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsit_ref_compnr), '0'), 38, 10) is null and 
                    src:tsit_ref_compnr is not null) THEN
                    'tsit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tsit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is null and 
                    src:twar_ref_compnr is not null) THEN
                    'twar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:twar_ref_compnr) || '\'' WHEN 
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
                                    
                src:date,
                src:item,
                src:cprj,
                src:sern,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPSS600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aitm_ref_compnr), '0'), 38, 10) is null and 
                    src:aitm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bsqn), '0'), 38, 10) is null and 
                    src:bsqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buyr_ref_compnr), '0'), 38, 10) is null and 
                    src:buyr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crbn), '0'), 38, 10) is null and 
                    src:crbn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:date), '1900-01-01')) is null and 
                    src:date is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:deli), '0'), 38, 10) is null and 
                    src:deli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsit_ref_compnr), '0'), 38, 10) is null and 
                    src:dsit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effw), '0'), 38, 10) is null and 
                    src:effw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effw_ref_compnr), '0'), 38, 10) is null and 
                    src:effw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph), '0'), 38, 10) is null and 
                    src:efph is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:efph_ref_compnr), '0'), 38, 10) is null and 
                    src:efph_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kotr), '0'), 38, 10) is null and 
                    src:kotr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phit_ref_compnr), '0'), 38, 10) is null and 
                    src:phit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is null and 
                    src:plid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quan), '0'), 38, 10) is null and 
                    src:quan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtdl), '0'), 38, 10) is null and 
                    src:rtdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tsit_ref_compnr), '0'), 38, 10) is null and 
                    src:tsit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is null and 
                    src:twar_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)