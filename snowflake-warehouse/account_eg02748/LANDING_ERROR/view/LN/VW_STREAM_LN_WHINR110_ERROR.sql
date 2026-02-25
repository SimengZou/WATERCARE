CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINR110_ERROR AS SELECT src, 'LN_WHINR110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) THEN
                    'boml contains a non-numeric value : \'' || AS_VARCHAR(src:boml) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) THEN
                    'chan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:chan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) THEN
                    'cinv contains a non-numeric value : \'' || AS_VARCHAR(src:cinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) THEN
                    'cons contains a non-numeric value : \'' || AS_VARCHAR(src:cons) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cosv), '0'), 38, 10) is null and 
                    src:cosv is not null) THEN
                    'cosv contains a non-numeric value : \'' || AS_VARCHAR(src:cosv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrr), '0'), 38, 10) is null and 
                    src:ctrr is not null) THEN
                    'ctrr contains a non-numeric value : \'' || AS_VARCHAR(src:ctrr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlin), '0'), 38, 10) is null and 
                    src:dlin is not null) THEN
                    'dlin contains a non-numeric value : \'' || AS_VARCHAR(src:dlin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ilgd), '1900-01-01')) is null and 
                    src:ilgd is not null) THEN
                    'ilgd contains a non-timestamp value : \'' || AS_VARCHAR(src:ilgd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iseq), '0'), 38, 10) is null and 
                    src:iseq is not null) THEN
                    'iseq contains a non-numeric value : \'' || AS_VARCHAR(src:iseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:itrd), '1900-01-01')) is null and 
                    src:itrd is not null) THEN
                    'itrd contains a non-timestamp value : \'' || AS_VARCHAR(src:itrd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itse), '0'), 38, 10) is null and 
                    src:itse is not null) THEN
                    'itse contains a non-numeric value : \'' || AS_VARCHAR(src:itse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) THEN
                    'koor contains a non-numeric value : \'' || AS_VARCHAR(src:koor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kost), '0'), 38, 10) is null and 
                    src:kost is not null) THEN
                    'kost contains a non-numeric value : \'' || AS_VARCHAR(src:kost) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lgdt), '1900-01-01')) is null and 
                    src:lgdt is not null) THEN
                    'lgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oats_ref_compnr), '0'), 38, 10) is null and 
                    src:oats_ref_compnr is not null) THEN
                    'oats_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oats_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ocmp_ref_compnr is not null) THEN
                    'ocmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) THEN
                    'owns contains a non-numeric value : \'' || AS_VARCHAR(src:owns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phtr), '0'), 38, 10) is null and 
                    src:phtr is not null) THEN
                    'phtr contains a non-numeric value : \'' || AS_VARCHAR(src:phtr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prjp), '0'), 38, 10) is null and 
                    src:prjp is not null) THEN
                    'prjp contains a non-numeric value : \'' || AS_VARCHAR(src:prjp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnt), '0'), 38, 10) is null and 
                    src:prnt is not null) THEN
                    'prnt contains a non-numeric value : \'' || AS_VARCHAR(src:prnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pyps), '0'), 38, 10) is null and 
                    src:pyps is not null) THEN
                    'pyps contains a non-numeric value : \'' || AS_VARCHAR(src:pyps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qaiu), '0'), 38, 10) is null and 
                    src:qaiu is not null) THEN
                    'qaiu contains a non-numeric value : \'' || AS_VARCHAR(src:qaiu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapu), '0'), 38, 10) is null and 
                    src:qapu is not null) THEN
                    'qapu contains a non-numeric value : \'' || AS_VARCHAR(src:qapu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd), '0'), 38, 10) is null and 
                    src:qhnd is not null) THEN
                    'qhnd contains a non-numeric value : \'' || AS_VARCHAR(src:qhnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buar), '0'), 38, 10) is null and 
                    src:qhnd_buar is not null) THEN
                    'qhnd_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qhnd_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buln), '0'), 38, 10) is null and 
                    src:qhnd_buln is not null) THEN
                    'qhnd_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qhnd_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buvl), '0'), 38, 10) is null and 
                    src:qhnd_buvl is not null) THEN
                    'qhnd_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qhnd_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buwg), '0'), 38, 10) is null and 
                    src:qhnd_buwg is not null) THEN
                    'qhnd_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qhnd_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qipu), '0'), 38, 10) is null and 
                    src:qipu is not null) THEN
                    'qipu contains a non-numeric value : \'' || AS_VARCHAR(src:qipu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk), '0'), 38, 10) is null and 
                    src:qstk is not null) THEN
                    'qstk contains a non-numeric value : \'' || AS_VARCHAR(src:qstk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buar), '0'), 38, 10) is null and 
                    src:qstk_buar is not null) THEN
                    'qstk_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qstk_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buln), '0'), 38, 10) is null and 
                    src:qstk_buln is not null) THEN
                    'qstk_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qstk_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buvl), '0'), 38, 10) is null and 
                    src:qstk_buvl is not null) THEN
                    'qstk_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qstk_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buwg), '0'), 38, 10) is null and 
                    src:qstk_buwg is not null) THEN
                    'qstk_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qstk_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) THEN
                    'rcln contains a non-numeric value : \'' || AS_VARCHAR(src:rcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) THEN
                    'reco contains a non-numeric value : \'' || AS_VARCHAR(src:reco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reje), '0'), 38, 10) is null and 
                    src:reje is not null) THEN
                    'reje contains a non-numeric value : \'' || AS_VARCHAR(src:reje) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpo), '0'), 38, 10) is null and 
                    src:shpo is not null) THEN
                    'shpo contains a non-numeric value : \'' || AS_VARCHAR(src:shpo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnb), '0'), 38, 10) is null and 
                    src:srnb is not null) THEN
                    'srnb contains a non-numeric value : \'' || AS_VARCHAR(src:srnb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:valm), '0'), 38, 10) is null and 
                    src:valm is not null) THEN
                    'valm contains a non-numeric value : \'' || AS_VARCHAR(src:valm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vbas), '0'), 38, 10) is null and 
                    src:vbas is not null) THEN
                    'vbas contains a non-numeric value : \'' || AS_VARCHAR(src:vbas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vwvg), '0'), 38, 10) is null and 
                    src:vwvg is not null) THEN
                    'vwvg contains a non-numeric value : \'' || AS_VARCHAR(src:vwvg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wvgr_ref_compnr), '0'), 38, 10) is null and 
                    src:wvgr_ref_compnr is not null) THEN
                    'wvgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wvgr_ref_compnr) || '\'' WHEN 
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
                                    
                src:item,
                src:seqn,
                src:trdt,
                src:cwar,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINR110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cosv), '0'), 38, 10) is null and 
                    src:cosv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrr), '0'), 38, 10) is null and 
                    src:ctrr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlin), '0'), 38, 10) is null and 
                    src:dlin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ilgd), '1900-01-01')) is null and 
                    src:ilgd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iseq), '0'), 38, 10) is null and 
                    src:iseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:itrd), '1900-01-01')) is null and 
                    src:itrd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itse), '0'), 38, 10) is null and 
                    src:itse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:koor), '0'), 38, 10) is null and 
                    src:koor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kost), '0'), 38, 10) is null and 
                    src:kost is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lgdt), '1900-01-01')) is null and 
                    src:lgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oats_ref_compnr), '0'), 38, 10) is null and 
                    src:oats_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ocmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owns), '0'), 38, 10) is null and 
                    src:owns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phtr), '0'), 38, 10) is null and 
                    src:phtr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prjp), '0'), 38, 10) is null and 
                    src:prjp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnt), '0'), 38, 10) is null and 
                    src:prnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pyps), '0'), 38, 10) is null and 
                    src:pyps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qaiu), '0'), 38, 10) is null and 
                    src:qaiu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapu), '0'), 38, 10) is null and 
                    src:qapu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd), '0'), 38, 10) is null and 
                    src:qhnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buar), '0'), 38, 10) is null and 
                    src:qhnd_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buln), '0'), 38, 10) is null and 
                    src:qhnd_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buvl), '0'), 38, 10) is null and 
                    src:qhnd_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd_buwg), '0'), 38, 10) is null and 
                    src:qhnd_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qipu), '0'), 38, 10) is null and 
                    src:qipu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk), '0'), 38, 10) is null and 
                    src:qstk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buar), '0'), 38, 10) is null and 
                    src:qstk_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buln), '0'), 38, 10) is null and 
                    src:qstk_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buvl), '0'), 38, 10) is null and 
                    src:qstk_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qstk_buwg), '0'), 38, 10) is null and 
                    src:qstk_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reje), '0'), 38, 10) is null and 
                    src:reje is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpo), '0'), 38, 10) is null and 
                    src:shpo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srnb), '0'), 38, 10) is null and 
                    src:srnb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:valm), '0'), 38, 10) is null and 
                    src:valm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vbas), '0'), 38, 10) is null and 
                    src:vbas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vwvg), '0'), 38, 10) is null and 
                    src:vwvg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wvgr_ref_compnr), '0'), 38, 10) is null and 
                    src:wvgr_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)