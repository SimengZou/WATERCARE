CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSMDM000_ERROR AS SELECT src, 'LN_TSMDM000' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alrm), '0'), 38, 10) is null and 
                    src:alrm is not null) THEN
                    'alrm contains a non-numeric value : \'' || AS_VARCHAR(src:alrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_1), '0'), 38, 10) is null and 
                    src:atws_1 is not null) THEN
                    'atws_1 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_10), '0'), 38, 10) is null and 
                    src:atws_10 is not null) THEN
                    'atws_10 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_2), '0'), 38, 10) is null and 
                    src:atws_2 is not null) THEN
                    'atws_2 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_3), '0'), 38, 10) is null and 
                    src:atws_3 is not null) THEN
                    'atws_3 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_4), '0'), 38, 10) is null and 
                    src:atws_4 is not null) THEN
                    'atws_4 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_5), '0'), 38, 10) is null and 
                    src:atws_5 is not null) THEN
                    'atws_5 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_6), '0'), 38, 10) is null and 
                    src:atws_6 is not null) THEN
                    'atws_6 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_7), '0'), 38, 10) is null and 
                    src:atws_7 is not null) THEN
                    'atws_7 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_8), '0'), 38, 10) is null and 
                    src:atws_8 is not null) THEN
                    'atws_8 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_9), '0'), 38, 10) is null and 
                    src:atws_9 is not null) THEN
                    'atws_9 contains a non-numeric value : \'' || AS_VARCHAR(src:atws_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccli), '0'), 38, 10) is null and 
                    src:ccli is not null) THEN
                    'ccli contains a non-numeric value : \'' || AS_VARCHAR(src:ccli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cibp), '0'), 38, 10) is null and 
                    src:cibp is not null) THEN
                    'cibp contains a non-numeric value : \'' || AS_VARCHAR(src:cibp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) THEN
                    'clgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) THEN
                    'clyn contains a non-numeric value : \'' || AS_VARCHAR(src:clyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdi_1), '0'), 38, 10) is null and 
                    src:cpdi_1 is not null) THEN
                    'cpdi_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cpdi_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdi_2), '0'), 38, 10) is null and 
                    src:cpdi_2 is not null) THEN
                    'cpdi_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cpdi_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdi_3), '0'), 38, 10) is null and 
                    src:cpdi_3 is not null) THEN
                    'cpdi_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cpdi_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctii), '0'), 38, 10) is null and 
                    src:ctii is not null) THEN
                    'ctii contains a non-numeric value : \'' || AS_VARCHAR(src:ctii) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpb), '0'), 38, 10) is null and 
                    src:dfpb is not null) THEN
                    'dfpb contains a non-numeric value : \'' || AS_VARCHAR(src:dfpb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipy), '0'), 38, 10) is null and 
                    src:dipy is not null) THEN
                    'dipy contains a non-numeric value : \'' || AS_VARCHAR(src:dipy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dist), '0'), 38, 10) is null and 
                    src:dist is not null) THEN
                    'dist contains a non-numeric value : \'' || AS_VARCHAR(src:dist) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erac), '0'), 38, 10) is null and 
                    src:erac is not null) THEN
                    'erac contains a non-numeric value : \'' || AS_VARCHAR(src:erac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erdc), '0'), 38, 10) is null and 
                    src:erdc is not null) THEN
                    'erdc contains a non-numeric value : \'' || AS_VARCHAR(src:erdc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feyn), '0'), 38, 10) is null and 
                    src:feyn is not null) THEN
                    'feyn contains a non-numeric value : \'' || AS_VARCHAR(src:feyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flds), '0'), 38, 10) is null and 
                    src:flds is not null) THEN
                    'flds contains a non-numeric value : \'' || AS_VARCHAR(src:flds) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grpl), '0'), 38, 10) is null and 
                    src:grpl is not null) THEN
                    'grpl contains a non-numeric value : \'' || AS_VARCHAR(src:grpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_1), '0'), 38, 10) is null and 
                    src:icpr_1 is not null) THEN
                    'icpr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:icpr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_2), '0'), 38, 10) is null and 
                    src:icpr_2 is not null) THEN
                    'icpr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:icpr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_3), '0'), 38, 10) is null and 
                    src:icpr_3 is not null) THEN
                    'icpr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:icpr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_4), '0'), 38, 10) is null and 
                    src:icpr_4 is not null) THEN
                    'icpr_4 contains a non-numeric value : \'' || AS_VARCHAR(src:icpr_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_5), '0'), 38, 10) is null and 
                    src:icpr_5 is not null) THEN
                    'icpr_5 contains a non-numeric value : \'' || AS_VARCHAR(src:icpr_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icrr), '0'), 38, 10) is null and 
                    src:icrr is not null) THEN
                    'icrr contains a non-numeric value : \'' || AS_VARCHAR(src:icrr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) THEN
                    'inby contains a non-numeric value : \'' || AS_VARCHAR(src:inby) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ingr), '0'), 38, 10) is null and 
                    src:ingr is not null) THEN
                    'ingr contains a non-numeric value : \'' || AS_VARCHAR(src:ingr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipsm), '0'), 38, 10) is null and 
                    src:ipsm is not null) THEN
                    'ipsm contains a non-numeric value : \'' || AS_VARCHAR(src:ipsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isdp), '0'), 38, 10) is null and 
                    src:isdp is not null) THEN
                    'isdp contains a non-numeric value : \'' || AS_VARCHAR(src:isdp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isom), '0'), 38, 10) is null and 
                    src:isom is not null) THEN
                    'isom contains a non-numeric value : \'' || AS_VARCHAR(src:isom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iwos), '0'), 38, 10) is null and 
                    src:iwos is not null) THEN
                    'iwos contains a non-numeric value : \'' || AS_VARCHAR(src:iwos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kpad), '0'), 38, 10) is null and 
                    src:kpad is not null) THEN
                    'kpad contains a non-numeric value : \'' || AS_VARCHAR(src:kpad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:meyn), '0'), 38, 10) is null and 
                    src:meyn is not null) THEN
                    'meyn contains a non-numeric value : \'' || AS_VARCHAR(src:meyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncri), '0'), 38, 10) is null and 
                    src:ncri is not null) THEN
                    'ncri contains a non-numeric value : \'' || AS_VARCHAR(src:ncri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngtb_ref_compnr), '0'), 38, 10) is null and 
                    src:ngtb_ref_compnr is not null) THEN
                    'ngtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngtb_sntb_ref_compnr), '0'), 38, 10) is null and 
                    src:ngtb_sntb_ref_compnr is not null) THEN
                    'ngtb_sntb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngtb_sntb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:npeg), '0'), 38, 10) is null and 
                    src:npeg is not null) THEN
                    'npeg contains a non-numeric value : \'' || AS_VARCHAR(src:npeg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) THEN
                    'pmnt contains a non-numeric value : \'' || AS_VARCHAR(src:pmnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnt), '0'), 38, 10) is null and 
                    src:prnt is not null) THEN
                    'prnt contains a non-numeric value : \'' || AS_VARCHAR(src:prnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pvap), '0'), 38, 10) is null and 
                    src:pvap is not null) THEN
                    'pvap contains a non-numeric value : \'' || AS_VARCHAR(src:pvap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) THEN
                    'ract_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ract_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt), '0'), 38, 10) is null and 
                    src:sbmt is not null) THEN
                    'sbmt contains a non-numeric value : \'' || AS_VARCHAR(src:sbmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scin), '0'), 38, 10) is null and 
                    src:scin is not null) THEN
                    'scin contains a non-numeric value : \'' || AS_VARCHAR(src:scin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scli), '0'), 38, 10) is null and 
                    src:scli is not null) THEN
                    'scli contains a non-numeric value : \'' || AS_VARCHAR(src:scli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpm), '0'), 38, 10) is null and 
                    src:shpm is not null) THEN
                    'shpm contains a non-numeric value : \'' || AS_VARCHAR(src:shpm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:siyn), '0'), 38, 10) is null and 
                    src:siyn is not null) THEN
                    'siyn contains a non-numeric value : \'' || AS_VARCHAR(src:siyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:skla), '0'), 38, 10) is null and 
                    src:skla is not null) THEN
                    'skla contains a non-numeric value : \'' || AS_VARCHAR(src:skla) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sklb), '0'), 38, 10) is null and 
                    src:sklb is not null) THEN
                    'sklb contains a non-numeric value : \'' || AS_VARCHAR(src:sklb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sklc), '0'), 38, 10) is null and 
                    src:sklc is not null) THEN
                    'sklc contains a non-numeric value : \'' || AS_VARCHAR(src:sklc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:skll), '0'), 38, 10) is null and 
                    src:skll is not null) THEN
                    'skll contains a non-numeric value : \'' || AS_VARCHAR(src:skll) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spdi), '0'), 38, 10) is null and 
                    src:spdi is not null) THEN
                    'spdi contains a non-numeric value : \'' || AS_VARCHAR(src:spdi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsm), '0'), 38, 10) is null and 
                    src:spsm is not null) THEN
                    'spsm contains a non-numeric value : \'' || AS_VARCHAR(src:spsm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlgi), '0'), 38, 10) is null and 
                    src:tlgi is not null) THEN
                    'tlgi contains a non-numeric value : \'' || AS_VARCHAR(src:tlgi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trac_1), '0'), 38, 10) is null and 
                    src:trac_1 is not null) THEN
                    'trac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:trac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trac_2), '0'), 38, 10) is null and 
                    src:trac_2 is not null) THEN
                    'trac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:trac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trac_3), '0'), 38, 10) is null and 
                    src:trac_3 is not null) THEN
                    'trac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:trac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tras), '0'), 38, 10) is null and 
                    src:tras is not null) THEN
                    'tras contains a non-numeric value : \'' || AS_VARCHAR(src:tras) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trmd), '0'), 38, 10) is null and 
                    src:trmd is not null) THEN
                    'trmd contains a non-numeric value : \'' || AS_VARCHAR(src:trmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ttpl), '0'), 38, 10) is null and 
                    src:ttpl is not null) THEN
                    'ttpl contains a non-numeric value : \'' || AS_VARCHAR(src:ttpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubpp), '0'), 38, 10) is null and 
                    src:ubpp is not null) THEN
                    'ubpp contains a non-numeric value : \'' || AS_VARCHAR(src:ubpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uday_ref_compnr), '0'), 38, 10) is null and 
                    src:uday_ref_compnr is not null) THEN
                    'uday_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uday_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udgn), '0'), 38, 10) is null and 
                    src:udgn is not null) THEN
                    'udgn contains a non-numeric value : \'' || AS_VARCHAR(src:udgn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umon_ref_compnr), '0'), 38, 10) is null and 
                    src:umon_ref_compnr is not null) THEN
                    'umon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:umon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:undi_ref_compnr), '0'), 38, 10) is null and 
                    src:undi_ref_compnr is not null) THEN
                    'undi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:undi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unlr_ref_compnr), '0'), 38, 10) is null and 
                    src:unlr_ref_compnr is not null) THEN
                    'unlr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unlr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:untd_ref_compnr), '0'), 38, 10) is null and 
                    src:untd_ref_compnr is not null) THEN
                    'untd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:untd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:untm_ref_compnr), '0'), 38, 10) is null and 
                    src:untm_ref_compnr is not null) THEN
                    'untm_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:untm_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usar), '0'), 38, 10) is null and 
                    src:usar is not null) THEN
                    'usar contains a non-numeric value : \'' || AS_VARCHAR(src:usar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uspg), '0'), 38, 10) is null and 
                    src:uspg is not null) THEN
                    'uspg contains a non-numeric value : \'' || AS_VARCHAR(src:uspg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utpd), '0'), 38, 10) is null and 
                    src:utpd is not null) THEN
                    'utpd contains a non-numeric value : \'' || AS_VARCHAR(src:utpd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwev), '0'), 38, 10) is null and 
                    src:uwev is not null) THEN
                    'uwev contains a non-numeric value : \'' || AS_VARCHAR(src:uwev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwks_ref_compnr), '0'), 38, 10) is null and 
                    src:uwks_ref_compnr is not null) THEN
                    'uwks_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uwks_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uyrs_ref_compnr), '0'), 38, 10) is null and 
                    src:uyrs_ref_compnr is not null) THEN
                    'uyrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uyrs_ref_compnr) || '\'' WHEN 
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
                                    
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSMDM000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alrm), '0'), 38, 10) is null and 
                    src:alrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_1), '0'), 38, 10) is null and 
                    src:atws_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_10), '0'), 38, 10) is null and 
                    src:atws_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_2), '0'), 38, 10) is null and 
                    src:atws_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_3), '0'), 38, 10) is null and 
                    src:atws_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_4), '0'), 38, 10) is null and 
                    src:atws_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_5), '0'), 38, 10) is null and 
                    src:atws_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_6), '0'), 38, 10) is null and 
                    src:atws_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_7), '0'), 38, 10) is null and 
                    src:atws_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_8), '0'), 38, 10) is null and 
                    src:atws_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atws_9), '0'), 38, 10) is null and 
                    src:atws_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccli), '0'), 38, 10) is null and 
                    src:ccli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cibp), '0'), 38, 10) is null and 
                    src:cibp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clyn), '0'), 38, 10) is null and 
                    src:clyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdi_1), '0'), 38, 10) is null and 
                    src:cpdi_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdi_2), '0'), 38, 10) is null and 
                    src:cpdi_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpdi_3), '0'), 38, 10) is null and 
                    src:cpdi_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctii), '0'), 38, 10) is null and 
                    src:ctii is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfpb), '0'), 38, 10) is null and 
                    src:dfpb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipy), '0'), 38, 10) is null and 
                    src:dipy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dist), '0'), 38, 10) is null and 
                    src:dist is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erac), '0'), 38, 10) is null and 
                    src:erac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erdc), '0'), 38, 10) is null and 
                    src:erdc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:feyn), '0'), 38, 10) is null and 
                    src:feyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flds), '0'), 38, 10) is null and 
                    src:flds is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grpl), '0'), 38, 10) is null and 
                    src:grpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_1), '0'), 38, 10) is null and 
                    src:icpr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_2), '0'), 38, 10) is null and 
                    src:icpr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_3), '0'), 38, 10) is null and 
                    src:icpr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_4), '0'), 38, 10) is null and 
                    src:icpr_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpr_5), '0'), 38, 10) is null and 
                    src:icpr_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icrr), '0'), 38, 10) is null and 
                    src:icrr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inby), '0'), 38, 10) is null and 
                    src:inby is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ingr), '0'), 38, 10) is null and 
                    src:ingr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipsm), '0'), 38, 10) is null and 
                    src:ipsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isdp), '0'), 38, 10) is null and 
                    src:isdp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isom), '0'), 38, 10) is null and 
                    src:isom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iwos), '0'), 38, 10) is null and 
                    src:iwos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kpad), '0'), 38, 10) is null and 
                    src:kpad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:meyn), '0'), 38, 10) is null and 
                    src:meyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncri), '0'), 38, 10) is null and 
                    src:ncri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngtb_ref_compnr), '0'), 38, 10) is null and 
                    src:ngtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngtb_sntb_ref_compnr), '0'), 38, 10) is null and 
                    src:ngtb_sntb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:npeg), '0'), 38, 10) is null and 
                    src:npeg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmnt), '0'), 38, 10) is null and 
                    src:pmnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnt), '0'), 38, 10) is null and 
                    src:prnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pvap), '0'), 38, 10) is null and 
                    src:pvap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt), '0'), 38, 10) is null and 
                    src:sbmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scin), '0'), 38, 10) is null and 
                    src:scin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scli), '0'), 38, 10) is null and 
                    src:scli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shpm), '0'), 38, 10) is null and 
                    src:shpm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:siyn), '0'), 38, 10) is null and 
                    src:siyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:skla), '0'), 38, 10) is null and 
                    src:skla is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sklb), '0'), 38, 10) is null and 
                    src:sklb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sklc), '0'), 38, 10) is null and 
                    src:sklc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:skll), '0'), 38, 10) is null and 
                    src:skll is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spdi), '0'), 38, 10) is null and 
                    src:spdi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsm), '0'), 38, 10) is null and 
                    src:spsm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlgi), '0'), 38, 10) is null and 
                    src:tlgi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trac_1), '0'), 38, 10) is null and 
                    src:trac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trac_2), '0'), 38, 10) is null and 
                    src:trac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trac_3), '0'), 38, 10) is null and 
                    src:trac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tras), '0'), 38, 10) is null and 
                    src:tras is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trmd), '0'), 38, 10) is null and 
                    src:trmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ttpl), '0'), 38, 10) is null and 
                    src:ttpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ubpp), '0'), 38, 10) is null and 
                    src:ubpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uday_ref_compnr), '0'), 38, 10) is null and 
                    src:uday_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udgn), '0'), 38, 10) is null and 
                    src:udgn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umon_ref_compnr), '0'), 38, 10) is null and 
                    src:umon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:undi_ref_compnr), '0'), 38, 10) is null and 
                    src:undi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unlr_ref_compnr), '0'), 38, 10) is null and 
                    src:unlr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:untd_ref_compnr), '0'), 38, 10) is null and 
                    src:untd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:untm_ref_compnr), '0'), 38, 10) is null and 
                    src:untm_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usar), '0'), 38, 10) is null and 
                    src:usar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uspg), '0'), 38, 10) is null and 
                    src:uspg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:utpd), '0'), 38, 10) is null and 
                    src:utpd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwev), '0'), 38, 10) is null and 
                    src:uwev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwks_ref_compnr), '0'), 38, 10) is null and 
                    src:uwks_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uyrs_ref_compnr), '0'), 38, 10) is null and 
                    src:uyrs_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)