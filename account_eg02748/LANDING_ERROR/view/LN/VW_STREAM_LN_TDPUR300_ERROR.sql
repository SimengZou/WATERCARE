CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR300_ERROR AS SELECT src, 'LN_TDPUR300' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) THEN
                    'ccon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) THEN
                    'cdat contains a non-timestamp value : \'' || AS_VARCHAR(src:cdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_aval), '0'), 38, 10) is null and 
                    src:cdf_aval is not null) THEN
                    'cdf_aval contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_aval) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_camt), '0'), 38, 10) is null and 
                    src:cdf_camt is not null) THEN
                    'cdf_camt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_camt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_comt), '0'), 38, 10) is null and 
                    src:cdf_comt is not null) THEN
                    'cdf_comt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_comt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ramt), '0'), 38, 10) is null and 
                    src:cdf_ramt is not null) THEN
                    'cdf_ramt contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_ramt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_vval), '0'), 38, 10) is null and 
                    src:cdf_vval is not null) THEN
                    'cdf_vval contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_vval) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chrq), '0'), 38, 10) is null and 
                    src:chrq is not null) THEN
                    'chrq contains a non-numeric value : \'' || AS_VARCHAR(src:chrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copc), '0'), 38, 10) is null and 
                    src:copc is not null) THEN
                    'copc contains a non-numeric value : \'' || AS_VARCHAR(src:copc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp_ref_compnr), '0'), 38, 10) is null and 
                    src:cotp_ref_compnr is not null) THEN
                    'cotp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cotp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht), '0'), 38, 10) is null and 
                    src:crht is not null) THEN
                    'crht contains a non-numeric value : \'' || AS_VARCHAR(src:crht) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht_ref_compnr), '0'), 38, 10) is null and 
                    src:crht_ref_compnr is not null) THEN
                    'crht_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crht_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crin), '0'), 38, 10) is null and 
                    src:crin is not null) THEN
                    'crin contains a non-numeric value : \'' || AS_VARCHAR(src:crin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrq), '0'), 38, 10) is null and 
                    src:crrq is not null) THEN
                    'crrq contains a non-numeric value : \'' || AS_VARCHAR(src:crrq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crst), '0'), 38, 10) is null and 
                    src:crst is not null) THEN
                    'crst contains a non-numeric value : \'' || AS_VARCHAR(src:crst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) THEN
                    'csts contains a non-numeric value : \'' || AS_VARCHAR(src:csts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) THEN
                    'edat contains a non-timestamp value : \'' || AS_VARCHAR(src:edat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icap), '0'), 38, 10) is null and 
                    src:icap is not null) THEN
                    'icap contains a non-numeric value : \'' || AS_VARCHAR(src:icap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr), '0'), 38, 10) is null and 
                    src:icpr is not null) THEN
                    'icpr contains a non-numeric value : \'' || AS_VARCHAR(src:icpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icyp), '0'), 38, 10) is null and 
                    src:icyp is not null) THEN
                    'icyp contains a non-numeric value : \'' || AS_VARCHAR(src:icyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifad_ref_compnr), '0'), 38, 10) is null and 
                    src:ifad_ref_compnr is not null) THEN
                    'ifad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ifcn_ref_compnr is not null) THEN
                    'ifcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nope), '0'), 38, 10) is null and 
                    src:nope is not null) THEN
                    'nope contains a non-numeric value : \'' || AS_VARCHAR(src:nope) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocon_ref_compnr), '0'), 38, 10) is null and 
                    src:ocon_ref_compnr is not null) THEN
                    'ocon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ocon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otad_ref_compnr), '0'), 38, 10) is null and 
                    src:otad_ref_compnr is not null) THEN
                    'otad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otcn_ref_compnr), '0'), 38, 10) is null and 
                    src:otcn_ref_compnr is not null) THEN
                    'otcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is null and 
                    src:ptad_ref_compnr is not null) THEN
                    'ptad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ptcn_ref_compnr is not null) THEN
                    'ptcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp), '0'), 38, 10) is null and 
                    src:rcmp is not null) THEN
                    'rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:rcmp_ref_compnr is not null) THEN
                    'rcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is null and 
                    src:rcsi_ref_compnr is not null) THEN
                    'rcsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rofc_ref_compnr), '0'), 38, 10) is null and 
                    src:rofc_ref_compnr is not null) THEN
                    'rofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) THEN
                    'sfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:sfcn_ref_compnr is not null) THEN
                    'sfcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trid_ref_compnr), '0'), 38, 10) is null and 
                    src:trid_ref_compnr is not null) THEN
                    'trid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:trid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt), '0'), 38, 10) is null and 
                    src:txtt is not null) THEN
                    'txtt contains a non-numeric value : \'' || AS_VARCHAR(src:txtt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt_ref_compnr), '0'), 38, 10) is null and 
                    src:txtt_ref_compnr is not null) THEN
                    'txtt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtt_ref_compnr) || '\'' WHEN 
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
                                    
                src:cono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR300 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cdat), '1900-01-01')) is null and 
                    src:cdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_aval), '0'), 38, 10) is null and 
                    src:cdf_aval is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_camt), '0'), 38, 10) is null and 
                    src:cdf_camt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_comt), '0'), 38, 10) is null and 
                    src:cdf_comt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ramt), '0'), 38, 10) is null and 
                    src:cdf_ramt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_vval), '0'), 38, 10) is null and 
                    src:cdf_vval is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chrq), '0'), 38, 10) is null and 
                    src:chrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copc), '0'), 38, 10) is null and 
                    src:copc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp_ref_compnr), '0'), 38, 10) is null and 
                    src:cotp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht), '0'), 38, 10) is null and 
                    src:crht is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crht_ref_compnr), '0'), 38, 10) is null and 
                    src:crht_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crin), '0'), 38, 10) is null and 
                    src:crin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrq), '0'), 38, 10) is null and 
                    src:crrq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crst), '0'), 38, 10) is null and 
                    src:crst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csts), '0'), 38, 10) is null and 
                    src:csts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edat), '1900-01-01')) is null and 
                    src:edat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icap), '0'), 38, 10) is null and 
                    src:icap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr), '0'), 38, 10) is null and 
                    src:icpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icyp), '0'), 38, 10) is null and 
                    src:icyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifad_ref_compnr), '0'), 38, 10) is null and 
                    src:ifad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ifcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nope), '0'), 38, 10) is null and 
                    src:nope is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocon_ref_compnr), '0'), 38, 10) is null and 
                    src:ocon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otad_ref_compnr), '0'), 38, 10) is null and 
                    src:otad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otcn_ref_compnr), '0'), 38, 10) is null and 
                    src:otcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is null and 
                    src:ptad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ptcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp), '0'), 38, 10) is null and 
                    src:rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:rcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is null and 
                    src:rcsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rofc_ref_compnr), '0'), 38, 10) is null and 
                    src:rofc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is null and 
                    src:sfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is null and 
                    src:sfcn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trid_ref_compnr), '0'), 38, 10) is null and 
                    src:trid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt), '0'), 38, 10) is null and 
                    src:txtt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtt_ref_compnr), '0'), 38, 10) is null and 
                    src:txtt_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)