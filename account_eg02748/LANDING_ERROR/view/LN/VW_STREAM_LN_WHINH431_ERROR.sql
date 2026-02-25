CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH431_ERROR AS SELECT src, 'LN_WHINH431' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfl), '0'), 38, 10) is null and 
                    src:acfl is not null) THEN
                    'acfl contains a non-numeric value : \'' || AS_VARCHAR(src:acfl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aclr), '0'), 38, 10) is null and 
                    src:aclr is not null) THEN
                    'aclr contains a non-numeric value : \'' || AS_VARCHAR(src:aclr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addc), '0'), 38, 10) is null and 
                    src:addc is not null) THEN
                    'addc contains a non-numeric value : \'' || AS_VARCHAR(src:addc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adjs), '0'), 38, 10) is null and 
                    src:adjs is not null) THEN
                    'adjs contains a non-numeric value : \'' || AS_VARCHAR(src:adjs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqty), '0'), 38, 10) is null and 
                    src:aqty is not null) THEN
                    'aqty contains a non-numeric value : \'' || AS_VARCHAR(src:aqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt), '0'), 38, 10) is null and 
                    src:atxt is not null) THEN
                    'atxt contains a non-numeric value : \'' || AS_VARCHAR(src:atxt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt_ref_compnr), '0'), 38, 10) is null and 
                    src:atxt_ref_compnr is not null) THEN
                    'atxt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atxt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) THEN
                    'bgxc contains a non-numeric value : \'' || AS_VARCHAR(src:bgxc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bolp), '0'), 38, 10) is null and 
                    src:bolp is not null) THEN
                    'bolp contains a non-numeric value : \'' || AS_VARCHAR(src:bolp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) THEN
                    'boml contains a non-numeric value : \'' || AS_VARCHAR(src:boml) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) THEN
                    'casi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) THEN
                    'cinv contains a non-numeric value : \'' || AS_VARCHAR(src:cinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) THEN
                    'cons contains a non-numeric value : \'' || AS_VARCHAR(src:cons) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is null and 
                    src:csvc_ref_compnr is not null) THEN
                    'csvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvl), '0'), 38, 10) is null and 
                    src:csvl is not null) THEN
                    'csvl contains a non-numeric value : \'' || AS_VARCHAR(src:csvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) THEN
                    'ctyo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_loca_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_loca_ref_compnr is not null) THEN
                    'cwar_loca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_loca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) THEN
                    'cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:darq), '0'), 38, 10) is null and 
                    src:darq is not null) THEN
                    'darq contains a non-numeric value : \'' || AS_VARCHAR(src:darq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dphu), '0'), 38, 10) is null and 
                    src:dphu is not null) THEN
                    'dphu contains a non-numeric value : \'' || AS_VARCHAR(src:dphu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtxt), '0'), 38, 10) is null and 
                    src:dtxt is not null) THEN
                    'dtxt contains a non-numeric value : \'' || AS_VARCHAR(src:dtxt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtxt_ref_compnr), '0'), 38, 10) is null and 
                    src:dtxt_ref_compnr is not null) THEN
                    'dtxt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dtxt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fisc), '0'), 38, 10) is null and 
                    src:fisc is not null) THEN
                    'fisc contains a non-numeric value : \'' || AS_VARCHAR(src:fisc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frsm), '0'), 38, 10) is null and 
                    src:frsm is not null) THEN
                    'frsm contains a non-numeric value : \'' || AS_VARCHAR(src:frsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grwt), '0'), 38, 10) is null and 
                    src:grwt is not null) THEN
                    'grwt contains a non-numeric value : \'' || AS_VARCHAR(src:grwt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hubc), '0'), 38, 10) is null and 
                    src:hubc is not null) THEN
                    'hubc contains a non-numeric value : \'' || AS_VARCHAR(src:hubc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) THEN
                    'huid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:huid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hupr), '0'), 38, 10) is null and 
                    src:hupr is not null) THEN
                    'hupr contains a non-numeric value : \'' || AS_VARCHAR(src:hupr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iadj), '0'), 38, 10) is null and 
                    src:iadj is not null) THEN
                    'iadj contains a non-numeric value : \'' || AS_VARCHAR(src:iadj) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:iadt), '1900-01-01')) is null and 
                    src:iadt is not null) THEN
                    'iadt contains a non-timestamp value : \'' || AS_VARCHAR(src:iadt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:idat), '1900-01-01')) is null and 
                    src:idat is not null) THEN
                    'idat contains a non-timestamp value : \'' || AS_VARCHAR(src:idat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ilgd), '1900-01-01')) is null and 
                    src:ilgd is not null) THEN
                    'ilgd contains a non-timestamp value : \'' || AS_VARCHAR(src:ilgd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) THEN
                    'item_clot_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_clot_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:itrd), '1900-01-01')) is null and 
                    src:itrd is not null) THEN
                    'itrd contains a non-timestamp value : \'' || AS_VARCHAR(src:itrd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:manl), '0'), 38, 10) is null and 
                    src:manl is not null) THEN
                    'manl contains a non-numeric value : \'' || AS_VARCHAR(src:manl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nspb), '0'), 38, 10) is null and 
                    src:nspb is not null) THEN
                    'nspb contains a non-numeric value : \'' || AS_VARCHAR(src:nspb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntwt), '0'), 38, 10) is null and 
                    src:ntwt is not null) THEN
                    'ntwt contains a non-numeric value : \'' || AS_VARCHAR(src:ntwt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbol), '0'), 38, 10) is null and 
                    src:pbol is not null) THEN
                    'pbol contains a non-numeric value : \'' || AS_VARCHAR(src:pbol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psde), '0'), 38, 10) is null and 
                    src:psde is not null) THEN
                    'psde contains a non-numeric value : \'' || AS_VARCHAR(src:psde) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwsq), '0'), 38, 10) is null and 
                    src:pwsq is not null) THEN
                    'pwsq contains a non-numeric value : \'' || AS_VARCHAR(src:pwsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) THEN
                    'qadv contains a non-numeric value : \'' || AS_VARCHAR(src:qadv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnpu), '0'), 38, 10) is null and 
                    src:qnpu is not null) THEN
                    'qnpu contains a non-numeric value : \'' || AS_VARCHAR(src:qnpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnsh), '0'), 38, 10) is null and 
                    src:qnsh is not null) THEN
                    'qnsh contains a non-numeric value : \'' || AS_VARCHAR(src:qnsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpic), '0'), 38, 10) is null and 
                    src:qpic is not null) THEN
                    'qpic contains a non-numeric value : \'' || AS_VARCHAR(src:qpic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppu), '0'), 38, 10) is null and 
                    src:qppu is not null) THEN
                    'qppu contains a non-numeric value : \'' || AS_VARCHAR(src:qppu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprv), '0'), 38, 10) is null and 
                    src:qprv is not null) THEN
                    'qprv contains a non-numeric value : \'' || AS_VARCHAR(src:qprv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrcp), '0'), 38, 10) is null and 
                    src:qrcp is not null) THEN
                    'qrcp contains a non-numeric value : \'' || AS_VARCHAR(src:qrcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qshp), '0'), 38, 10) is null and 
                    src:qshp is not null) THEN
                    'qshp contains a non-numeric value : \'' || AS_VARCHAR(src:qshp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspu), '0'), 38, 10) is null and 
                    src:qspu is not null) THEN
                    'qspu contains a non-numeric value : \'' || AS_VARCHAR(src:qspu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recd_ref_compnr), '0'), 38, 10) is null and 
                    src:recd_ref_compnr is not null) THEN
                    'recd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:recd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqty), '0'), 38, 10) is null and 
                    src:rqty is not null) THEN
                    'rqty contains a non-numeric value : \'' || AS_VARCHAR(src:rqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sarq), '0'), 38, 10) is null and 
                    src:sarq is not null) THEN
                    'sarq contains a non-numeric value : \'' || AS_VARCHAR(src:sarq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serp), '0'), 38, 10) is null and 
                    src:serp is not null) THEN
                    'serp contains a non-numeric value : \'' || AS_VARCHAR(src:serp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpm_ref_compnr), '0'), 38, 10) is null and 
                    src:shpm_ref_compnr is not null) THEN
                    'shpm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:shpm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shst), '0'), 38, 10) is null and 
                    src:shst is not null) THEN
                    'shst contains a non-numeric value : \'' || AS_VARCHAR(src:shst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvl), '0'), 38, 10) is null and 
                    src:shvl is not null) THEN
                    'shvl contains a non-numeric value : \'' || AS_VARCHAR(src:shvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sppk), '0'), 38, 10) is null and 
                    src:sppk is not null) THEN
                    'sppk contains a non-numeric value : \'' || AS_VARCHAR(src:sppk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssqd), '0'), 38, 10) is null and 
                    src:ssqd is not null) THEN
                    'ssqd contains a non-numeric value : \'' || AS_VARCHAR(src:ssqd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) THEN
                    'stad_dlpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_dlpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) THEN
                    'tcst contains a non-numeric value : \'' || AS_VARCHAR(src:tcst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:worg), '0'), 38, 10) is null and 
                    src:worg is not null) THEN
                    'worg contains a non-numeric value : \'' || AS_VARCHAR(src:worg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:worn_wpon_pwsq_ssqd_ref_compnr), '0'), 38, 10) is null and 
                    src:worn_wpon_pwsq_ssqd_ref_compnr is not null) THEN
                    'worn_wpon_pwsq_ssqd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:worn_wpon_pwsq_ssqd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wpon), '0'), 38, 10) is null and 
                    src:wpon is not null) THEN
                    'wpon contains a non-numeric value : \'' || AS_VARCHAR(src:wpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wseq), '0'), 38, 10) is null and 
                    src:wseq is not null) THEN
                    'wseq contains a non-numeric value : \'' || AS_VARCHAR(src:wseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wset), '0'), 38, 10) is null and 
                    src:wset is not null) THEN
                    'wset contains a non-numeric value : \'' || AS_VARCHAR(src:wset) || '\'' WHEN 
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
                                    
                src:shpm,
                src:pono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH431 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfl), '0'), 38, 10) is null and 
                    src:acfl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aclr), '0'), 38, 10) is null and 
                    src:aclr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:addc), '0'), 38, 10) is null and 
                    src:addc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adjs), '0'), 38, 10) is null and 
                    src:adjs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqty), '0'), 38, 10) is null and 
                    src:aqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt), '0'), 38, 10) is null and 
                    src:atxt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atxt_ref_compnr), '0'), 38, 10) is null and 
                    src:atxt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bolp), '0'), 38, 10) is null and 
                    src:bolp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is null and 
                    src:csvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvl), '0'), 38, 10) is null and 
                    src:csvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_loca_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_loca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:darq), '0'), 38, 10) is null and 
                    src:darq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dphu), '0'), 38, 10) is null and 
                    src:dphu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtxt), '0'), 38, 10) is null and 
                    src:dtxt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtxt_ref_compnr), '0'), 38, 10) is null and 
                    src:dtxt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fisc), '0'), 38, 10) is null and 
                    src:fisc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frsm), '0'), 38, 10) is null and 
                    src:frsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grwt), '0'), 38, 10) is null and 
                    src:grwt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hubc), '0'), 38, 10) is null and 
                    src:hubc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hupr), '0'), 38, 10) is null and 
                    src:hupr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iadj), '0'), 38, 10) is null and 
                    src:iadj is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:iadt), '1900-01-01')) is null and 
                    src:iadt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:idat), '1900-01-01')) is null and 
                    src:idat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ilgd), '1900-01-01')) is null and 
                    src:ilgd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is null and 
                    src:item_clot_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:itrd), '1900-01-01')) is null and 
                    src:itrd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:manl), '0'), 38, 10) is null and 
                    src:manl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nspb), '0'), 38, 10) is null and 
                    src:nspb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntwt), '0'), 38, 10) is null and 
                    src:ntwt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbol), '0'), 38, 10) is null and 
                    src:pbol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psde), '0'), 38, 10) is null and 
                    src:psde is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwsq), '0'), 38, 10) is null and 
                    src:pwsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnpu), '0'), 38, 10) is null and 
                    src:qnpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnsh), '0'), 38, 10) is null and 
                    src:qnsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpic), '0'), 38, 10) is null and 
                    src:qpic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppu), '0'), 38, 10) is null and 
                    src:qppu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprv), '0'), 38, 10) is null and 
                    src:qprv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrcp), '0'), 38, 10) is null and 
                    src:qrcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qshp), '0'), 38, 10) is null and 
                    src:qshp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qspu), '0'), 38, 10) is null and 
                    src:qspu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recd_ref_compnr), '0'), 38, 10) is null and 
                    src:recd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rqty), '0'), 38, 10) is null and 
                    src:rqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sarq), '0'), 38, 10) is null and 
                    src:sarq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serp), '0'), 38, 10) is null and 
                    src:serp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpm_ref_compnr), '0'), 38, 10) is null and 
                    src:shpm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shst), '0'), 38, 10) is null and 
                    src:shst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shvl), '0'), 38, 10) is null and 
                    src:shvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sppk), '0'), 38, 10) is null and 
                    src:sppk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssqd), '0'), 38, 10) is null and 
                    src:ssqd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_dlpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:worg), '0'), 38, 10) is null and 
                    src:worg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:worn_wpon_pwsq_ssqd_ref_compnr), '0'), 38, 10) is null and 
                    src:worn_wpon_pwsq_ssqd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wpon), '0'), 38, 10) is null and 
                    src:wpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wseq), '0'), 38, 10) is null and 
                    src:wseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wset), '0'), 38, 10) is null and 
                    src:wset is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)