CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM112_ERROR AS SELECT src, 'LN_TCCOM112' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adam), '0'), 38, 10) is null and 
                    src:adam is not null) THEN
                    'adam contains a non-numeric value : \'' || AS_VARCHAR(src:adam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpc), '0'), 38, 10) is null and 
                    src:adpc is not null) THEN
                    'adpc contains a non-numeric value : \'' || AS_VARCHAR(src:adpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bicy_ref_compnr), '0'), 38, 10) is null and 
                    src:bicy_ref_compnr is not null) THEN
                    'bicy_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bicy_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) THEN
                    'cbps_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbps_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) THEN
                    'cbtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccic_ref_compnr), '0'), 38, 10) is null and 
                    src:ccic_ref_compnr is not null) THEN
                    'ccic_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccic_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) THEN
                    'ccnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccra_ref_compnr), '0'), 38, 10) is null and 
                    src:ccra_ref_compnr is not null) THEN
                    'ccra_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccra_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfcg_rcmp), '0'), 38, 10) is null and 
                    src:cfcg_rcmp is not null) THEN
                    'cfcg_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cfcg_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chin), '0'), 38, 10) is null and 
                    src:chin is not null) THEN
                    'chin contains a non-numeric value : \'' || AS_VARCHAR(src:chin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is null and 
                    src:cidm_ref_compnr is not null) THEN
                    'cidm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cidm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) THEN
                    'cinm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cinm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) THEN
                    'clin contains a non-numeric value : \'' || AS_VARCHAR(src:clin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clmt_ref_compnr), '0'), 38, 10) is null and 
                    src:clmt_ref_compnr is not null) THEN
                    'clmt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clmt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crat_ref_compnr), '0'), 38, 10) is null and 
                    src:crat_ref_compnr is not null) THEN
                    'crat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crlr), '0'), 38, 10) is null and 
                    src:crlr is not null) THEN
                    'crlr contains a non-numeric value : \'' || AS_VARCHAR(src:crlr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlcr), '1900-01-01')) is null and 
                    src:dlcr is not null) THEN
                    'dlcr contains a non-timestamp value : \'' || AS_VARCHAR(src:dlcr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edci), '1900-01-01')) is null and 
                    src:edci is not null) THEN
                    'edci contains a non-timestamp value : \'' || AS_VARCHAR(src:edci) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hcru), '0'), 38, 10) is null and 
                    src:hcru is not null) THEN
                    'hcru contains a non-numeric value : \'' || AS_VARCHAR(src:hcru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infq), '0'), 38, 10) is null and 
                    src:infq is not null) THEN
                    'infq contains a non-numeric value : \'' || AS_VARCHAR(src:infq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infr), '0'), 38, 10) is null and 
                    src:infr is not null) THEN
                    'infr contains a non-numeric value : \'' || AS_VARCHAR(src:infr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) THEN
                    'inpl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:inpl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itst), '0'), 38, 10) is null and 
                    src:itst is not null) THEN
                    'itst contains a non-numeric value : \'' || AS_VARCHAR(src:itst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lidt), '1900-01-01')) is null and 
                    src:lidt is not null) THEN
                    'lidt contains a non-timestamp value : \'' || AS_VARCHAR(src:lidt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkst), '0'), 38, 10) is null and 
                    src:lkst is not null) THEN
                    'lkst contains a non-numeric value : \'' || AS_VARCHAR(src:lkst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcod_ref_compnr), '0'), 38, 10) is null and 
                    src:mcod_ref_compnr is not null) THEN
                    'mcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncin), '0'), 38, 10) is null and 
                    src:ncin is not null) THEN
                    'ncin contains a non-numeric value : \'' || AS_VARCHAR(src:ncin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrbl), '0'), 38, 10) is null and 
                    src:qrbl is not null) THEN
                    'qrbl contains a non-numeric value : \'' || AS_VARCHAR(src:qrbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:rcrs_ref_compnr is not null) THEN
                    'rcrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpay_ref_compnr), '0'), 38, 10) is null and 
                    src:rpay_ref_compnr is not null) THEN
                    'rpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tamd), '0'), 38, 10) is null and 
                    src:tamd is not null) THEN
                    'tamd contains a non-numeric value : \'' || AS_VARCHAR(src:tamd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpcd), '0'), 38, 10) is null and 
                    src:tpcd is not null) THEN
                    'tpcd contains a non-numeric value : \'' || AS_VARCHAR(src:tpcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                                    
                src:cofc,
                src:compnr,
                src:itbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM112 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adam), '0'), 38, 10) is null and 
                    src:adam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adpc), '0'), 38, 10) is null and 
                    src:adpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bicy_ref_compnr), '0'), 38, 10) is null and 
                    src:bicy_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccic_ref_compnr), '0'), 38, 10) is null and 
                    src:ccic_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccra_ref_compnr), '0'), 38, 10) is null and 
                    src:ccra_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfcg_rcmp), '0'), 38, 10) is null and 
                    src:cfcg_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chin), '0'), 38, 10) is null and 
                    src:chin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is null and 
                    src:cidm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is null and 
                    src:cinm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clmt_ref_compnr), '0'), 38, 10) is null and 
                    src:clmt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crat_ref_compnr), '0'), 38, 10) is null and 
                    src:crat_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crlr), '0'), 38, 10) is null and 
                    src:crlr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlcr), '1900-01-01')) is null and 
                    src:dlcr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edci), '1900-01-01')) is null and 
                    src:edci is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hcru), '0'), 38, 10) is null and 
                    src:hcru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infq), '0'), 38, 10) is null and 
                    src:infq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:infr), '0'), 38, 10) is null and 
                    src:infr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is null and 
                    src:inpl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itst), '0'), 38, 10) is null and 
                    src:itst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lidt), '1900-01-01')) is null and 
                    src:lidt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lkst), '0'), 38, 10) is null and 
                    src:lkst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcod_ref_compnr), '0'), 38, 10) is null and 
                    src:mcod_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncin), '0'), 38, 10) is null and 
                    src:ncin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrbl), '0'), 38, 10) is null and 
                    src:qrbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:rcrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpay_ref_compnr), '0'), 38, 10) is null and 
                    src:rpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tamd), '0'), 38, 10) is null and 
                    src:tamd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpcd), '0'), 38, 10) is null and 
                    src:tpcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)