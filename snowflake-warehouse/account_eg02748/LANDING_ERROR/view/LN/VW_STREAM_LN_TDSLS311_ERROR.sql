CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSLS311_ERROR AS SELECT src, 'LN_TDSLS311' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:ccof_ref_compnr is not null) THEN
                    'ccof_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccof_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) THEN
                    'cfrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) THEN
                    'citt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_pono_ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_pono_ccof_ref_compnr is not null) THEN
                    'cono_pono_ccof_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_pono_ccof_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) THEN
                    'cono_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) THEN
                    'cons contains a non-numeric value : \'' || AS_VARCHAR(src:cons) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) THEN
                    'corg contains a non-numeric value : \'' || AS_VARCHAR(src:corg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:csdt), '1900-01-01')) is null and 
                    src:csdt is not null) THEN
                    'csdt contains a non-timestamp value : \'' || AS_VARCHAR(src:csdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) THEN
                    'delc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:delc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dest), '0'), 38, 10) is null and 
                    src:dest is not null) THEN
                    'dest contains a non-numeric value : \'' || AS_VARCHAR(src:dest) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edfb), '1900-01-01')) is null and 
                    src:edfb is not null) THEN
                    'edfb contains a non-timestamp value : \'' || AS_VARCHAR(src:edfb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edrw), '1900-01-01')) is null and 
                    src:edrw is not null) THEN
                    'edrw contains a non-timestamp value : \'' || AS_VARCHAR(src:edrw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:excp), '0'), 38, 10) is null and 
                    src:excp is not null) THEN
                    'excp contains a non-numeric value : \'' || AS_VARCHAR(src:excp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) THEN
                    'fcrt contains a non-numeric value : \'' || AS_VARCHAR(src:fcrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) THEN
                    'fdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:gdat), '1900-01-01')) is null and 
                    src:gdat is not null) THEN
                    'gdat contains a non-timestamp value : \'' || AS_VARCHAR(src:gdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiss), '0'), 38, 10) is null and 
                    src:hiss is not null) THEN
                    'hiss contains a non-numeric value : \'' || AS_VARCHAR(src:hiss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hold), '0'), 38, 10) is null and 
                    src:hold is not null) THEN
                    'hold contains a non-numeric value : \'' || AS_VARCHAR(src:hold) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) THEN
                    'itad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) THEN
                    'item_site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lrdt), '1900-01-01')) is null and 
                    src:lrdt is not null) THEN
                    'lrdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lrdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) THEN
                    'lsel contains a non-numeric value : \'' || AS_VARCHAR(src:lsel) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltcs), '0'), 38, 10) is null and 
                    src:ltcs is not null) THEN
                    'ltcs contains a non-numeric value : \'' || AS_VARCHAR(src:ltcs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) THEN
                    'motv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:motv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) THEN
                    'odis contains a non-numeric value : \'' || AS_VARCHAR(src:odis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) THEN
                    'ofad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) THEN
                    'ofcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) THEN
                    'osrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:osrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) THEN
                    'pfad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) THEN
                    'pfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plan_ref_compnr), '0'), 38, 10) is null and 
                    src:plan_ref_compnr is not null) THEN
                    'plan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qici), '0'), 38, 10) is null and 
                    src:qici is not null) THEN
                    'qici contains a non-numeric value : \'' || AS_VARCHAR(src:qici) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicp), '0'), 38, 10) is null and 
                    src:qicp is not null) THEN
                    'qicp contains a non-numeric value : \'' || AS_VARCHAR(src:qicp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicr), '0'), 38, 10) is null and 
                    src:qicr is not null) THEN
                    'qicr contains a non-numeric value : \'' || AS_VARCHAR(src:qicr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qics), '0'), 38, 10) is null and 
                    src:qics is not null) THEN
                    'qics contains a non-numeric value : \'' || AS_VARCHAR(src:qics) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qifa), '0'), 38, 10) is null and 
                    src:qifa is not null) THEN
                    'qifa contains a non-numeric value : \'' || AS_VARCHAR(src:qifa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qifh), '0'), 38, 10) is null and 
                    src:qifh is not null) THEN
                    'qifh contains a non-numeric value : \'' || AS_VARCHAR(src:qifh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qilr), '0'), 38, 10) is null and 
                    src:qilr is not null) THEN
                    'qilr contains a non-numeric value : \'' || AS_VARCHAR(src:qilr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qira), '0'), 38, 10) is null and 
                    src:qira is not null) THEN
                    'qira contains a non-numeric value : \'' || AS_VARCHAR(src:qira) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirh), '0'), 38, 10) is null and 
                    src:qirh is not null) THEN
                    'qirh contains a non-numeric value : \'' || AS_VARCHAR(src:qirh) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_1), '0'), 38, 10) is null and 
                    src:rats_1 is not null) THEN
                    'rats_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_2), '0'), 38, 10) is null and 
                    src:rats_2 is not null) THEN
                    'rats_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_3), '0'), 38, 10) is null and 
                    src:rats_3 is not null) THEN
                    'rats_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rats_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is null and 
                    src:ratt_ref_compnr is not null) THEN
                    'ratt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ratt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsiu), '0'), 38, 10) is null and 
                    src:rsiu is not null) THEN
                    'rsiu contains a non-numeric value : \'' || AS_VARCHAR(src:rsiu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rviu), '0'), 38, 10) is null and 
                    src:rviu is not null) THEN
                    'rviu contains a non-numeric value : \'' || AS_VARCHAR(src:rviu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) THEN
                    'sbim contains a non-numeric value : \'' || AS_VARCHAR(src:sbim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sctp), '0'), 38, 10) is null and 
                    src:sctp is not null) THEN
                    'sctp contains a non-numeric value : \'' || AS_VARCHAR(src:sctp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) THEN
                    'sdsc contains a non-numeric value : \'' || AS_VARCHAR(src:sdsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sllr_ref_compnr), '0'), 38, 10) is null and 
                    src:sllr_ref_compnr is not null) THEN
                    'sllr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sllr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sodb), '0'), 38, 10) is null and 
                    src:sodb is not null) THEN
                    'sodb contains a non-numeric value : \'' || AS_VARCHAR(src:sodb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) THEN
                    'stad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) THEN
                    'stcn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stcn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strc_ref_compnr), '0'), 38, 10) is null and 
                    src:strc_ref_compnr is not null) THEN
                    'strc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:strc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucms), '0'), 38, 10) is null and 
                    src:ucms is not null) THEN
                    'ucms contains a non-numeric value : \'' || AS_VARCHAR(src:ucms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucos), '0'), 38, 10) is null and 
                    src:ucos is not null) THEN
                    'ucos contains a non-numeric value : \'' || AS_VARCHAR(src:ucos) || '\'' WHEN 
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
                                    
                src:schn,
                src:sctp,
                src:revn,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS311 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:ccof_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is null and 
                    src:citt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_pono_ccof_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_pono_ccof_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:csdt), '1900-01-01')) is null and 
                    src:csdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dest), '0'), 38, 10) is null and 
                    src:dest is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edfb), '1900-01-01')) is null and 
                    src:edfb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:edrw), '1900-01-01')) is null and 
                    src:edrw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:excp), '0'), 38, 10) is null and 
                    src:excp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcrt), '0'), 38, 10) is null and 
                    src:fcrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:fdpt_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:gdat), '1900-01-01')) is null and 
                    src:gdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hiss), '0'), 38, 10) is null and 
                    src:hiss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hold), '0'), 38, 10) is null and 
                    src:hold is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is null and 
                    src:itad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is null and 
                    src:item_site_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lrdt), '1900-01-01')) is null and 
                    src:lrdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsel), '0'), 38, 10) is null and 
                    src:lsel is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltcs), '0'), 38, 10) is null and 
                    src:ltcs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is null and 
                    src:ofad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is null and 
                    src:ofcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is null and 
                    src:pfad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:pfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plan_ref_compnr), '0'), 38, 10) is null and 
                    src:plan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qici), '0'), 38, 10) is null and 
                    src:qici is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicp), '0'), 38, 10) is null and 
                    src:qicp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qicr), '0'), 38, 10) is null and 
                    src:qicr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qics), '0'), 38, 10) is null and 
                    src:qics is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qifa), '0'), 38, 10) is null and 
                    src:qifa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qifh), '0'), 38, 10) is null and 
                    src:qifh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qilr), '0'), 38, 10) is null and 
                    src:qilr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qira), '0'), 38, 10) is null and 
                    src:qira is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirh), '0'), 38, 10) is null and 
                    src:qirh is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_1), '0'), 38, 10) is null and 
                    src:rats_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_2), '0'), 38, 10) is null and 
                    src:rats_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats_3), '0'), 38, 10) is null and 
                    src:rats_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is null and 
                    src:ratt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsiu), '0'), 38, 10) is null and 
                    src:rsiu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rviu), '0'), 38, 10) is null and 
                    src:rviu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbim), '0'), 38, 10) is null and 
                    src:sbim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sctp), '0'), 38, 10) is null and 
                    src:sctp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdsc), '0'), 38, 10) is null and 
                    src:sdsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sllr_ref_compnr), '0'), 38, 10) is null and 
                    src:sllr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sodb), '0'), 38, 10) is null and 
                    src:sodb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is null and 
                    src:stad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is null and 
                    src:stcn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strc_ref_compnr), '0'), 38, 10) is null and 
                    src:strc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucms), '0'), 38, 10) is null and 
                    src:ucms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucos), '0'), 38, 10) is null and 
                    src:ucos is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)