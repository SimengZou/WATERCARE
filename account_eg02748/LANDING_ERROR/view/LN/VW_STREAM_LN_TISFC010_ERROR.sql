CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TISFC010_ERROR AS SELECT src, 'LN_TISFC010' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ardt), '1900-01-01')) is null and 
                    src:ardt is not null) THEN
                    'ardt contains a non-timestamp value : \'' || AS_VARCHAR(src:ardt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:asdt), '1900-01-01')) is null and 
                    src:asdt is not null) THEN
                    'asdt contains a non-timestamp value : \'' || AS_VARCHAR(src:asdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfls), '0'), 38, 10) is null and 
                    src:bfls is not null) THEN
                    'bfls contains a non-numeric value : \'' || AS_VARCHAR(src:bfls) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blcd_ref_compnr), '0'), 38, 10) is null and 
                    src:blcd_ref_compnr is not null) THEN
                    'blcd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:blcd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctt), '0'), 38, 10) is null and 
                    src:cctt is not null) THEN
                    'cctt contains a non-numeric value : \'' || AS_VARCHAR(src:cctt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) THEN
                    'cmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copo), '0'), 38, 10) is null and 
                    src:copo is not null) THEN
                    'copo contains a non-numeric value : \'' || AS_VARCHAR(src:copo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmso), '0'), 38, 10) is null and 
                    src:dmso is not null) THEN
                    'dmso contains a non-numeric value : \'' || AS_VARCHAR(src:dmso) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:esdt), '1900-01-01')) is null and 
                    src:esdt is not null) THEN
                    'esdt contains a non-timestamp value : \'' || AS_VARCHAR(src:esdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdur), '0'), 38, 10) is null and 
                    src:fdur is not null) THEN
                    'fdur contains a non-numeric value : \'' || AS_VARCHAR(src:fdur) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fidt), '1900-01-01')) is null and 
                    src:fidt is not null) THEN
                    'fidt contains a non-timestamp value : \'' || AS_VARCHAR(src:fidt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxpd), '0'), 38, 10) is null and 
                    src:fxpd is not null) THEN
                    'fxpd contains a non-numeric value : \'' || AS_VARCHAR(src:fxpd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxsu), '0'), 38, 10) is null and 
                    src:fxsu is not null) THEN
                    'fxsu contains a non-numeric value : \'' || AS_VARCHAR(src:fxsu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lfdt), '1900-01-01')) is null and 
                    src:lfdt is not null) THEN
                    'lfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maho), '0'), 38, 10) is null and 
                    src:maho is not null) THEN
                    'maho contains a non-numeric value : \'' || AS_VARCHAR(src:maho) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcho), '0'), 38, 10) is null and 
                    src:mcho is not null) THEN
                    'mcho contains a non-numeric value : \'' || AS_VARCHAR(src:mcho) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcno_ref_compnr), '0'), 38, 10) is null and 
                    src:mcno_ref_compnr is not null) THEN
                    'mcno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcoc), '0'), 38, 10) is null and 
                    src:mcoc is not null) THEN
                    'mcoc contains a non-numeric value : \'' || AS_VARCHAR(src:mcoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnrs), '0'), 38, 10) is null and 
                    src:mnrs is not null) THEN
                    'mnrs contains a non-numeric value : \'' || AS_VARCHAR(src:mnrs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopr), '0'), 38, 10) is null and 
                    src:mopr is not null) THEN
                    'mopr contains a non-numeric value : \'' || AS_VARCHAR(src:mopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:most), '0'), 38, 10) is null and 
                    src:most is not null) THEN
                    'most contains a non-numeric value : \'' || AS_VARCHAR(src:most) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:mtyp_ref_compnr is not null) THEN
                    'mtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mvrd), '1900-01-01')) is null and 
                    src:mvrd is not null) THEN
                    'mvrd contains a non-timestamp value : \'' || AS_VARCHAR(src:mvrd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvtm), '0'), 38, 10) is null and 
                    src:mvtm is not null) THEN
                    'mvtm contains a non-numeric value : \'' || AS_VARCHAR(src:mvtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nnts), '0'), 38, 10) is null and 
                    src:nnts is not null) THEN
                    'nnts contains a non-numeric value : \'' || AS_VARCHAR(src:nnts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nomc), '0'), 38, 10) is null and 
                    src:nomc is not null) THEN
                    'nomc contains a non-numeric value : \'' || AS_VARCHAR(src:nomc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nopr), '0'), 38, 10) is null and 
                    src:nopr is not null) THEN
                    'nopr contains a non-numeric value : \'' || AS_VARCHAR(src:nopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oopn), '0'), 38, 10) is null and 
                    src:oopn is not null) THEN
                    'oopn contains a non-numeric value : \'' || AS_VARCHAR(src:oopn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opno), '0'), 38, 10) is null and 
                    src:opno is not null) THEN
                    'opno contains a non-numeric value : \'' || AS_VARCHAR(src:opno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opst), '0'), 38, 10) is null and 
                    src:opst is not null) THEN
                    'opst contains a non-numeric value : \'' || AS_VARCHAR(src:opst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) THEN
                    'oset contains a non-numeric value : \'' || AS_VARCHAR(src:oset) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) THEN
                    'pdno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pdno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pfdt), '1900-01-01')) is null and 
                    src:pfdt is not null) THEN
                    'pfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:pfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp), '0'), 38, 10) is null and 
                    src:phsp is not null) THEN
                    'phsp contains a non-numeric value : \'' || AS_VARCHAR(src:phsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buar), '0'), 38, 10) is null and 
                    src:phsp_buar is not null) THEN
                    'phsp_buar contains a non-numeric value : \'' || AS_VARCHAR(src:phsp_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buln), '0'), 38, 10) is null and 
                    src:phsp_buln is not null) THEN
                    'phsp_buln contains a non-numeric value : \'' || AS_VARCHAR(src:phsp_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_bupc), '0'), 38, 10) is null and 
                    src:phsp_bupc is not null) THEN
                    'phsp_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:phsp_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_butm), '0'), 38, 10) is null and 
                    src:phsp_butm is not null) THEN
                    'phsp_butm contains a non-numeric value : \'' || AS_VARCHAR(src:phsp_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buvl), '0'), 38, 10) is null and 
                    src:phsp_buvl is not null) THEN
                    'phsp_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:phsp_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buwg), '0'), 38, 10) is null and 
                    src:phsp_buwg is not null) THEN
                    'phsp_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:phsp_buwg) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prrd), '1900-01-01')) is null and 
                    src:prrd is not null) THEN
                    'prrd contains a non-timestamp value : \'' || AS_VARCHAR(src:prrd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prte), '0'), 38, 10) is null and 
                    src:prte is not null) THEN
                    'prte contains a non-numeric value : \'' || AS_VARCHAR(src:prte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtm), '0'), 38, 10) is null and 
                    src:prtm is not null) THEN
                    'prtm contains a non-numeric value : \'' || AS_VARCHAR(src:prtm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:psdt), '1900-01-01')) is null and 
                    src:psdt is not null) THEN
                    'psdt contains a non-timestamp value : \'' || AS_VARCHAR(src:psdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd), '0'), 38, 10) is null and 
                    src:qbfd is not null) THEN
                    'qbfd contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buar), '0'), 38, 10) is null and 
                    src:qbfd_buar is not null) THEN
                    'qbfd_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buln), '0'), 38, 10) is null and 
                    src:qbfd_buln is not null) THEN
                    'qbfd_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_bupc), '0'), 38, 10) is null and 
                    src:qbfd_bupc is not null) THEN
                    'qbfd_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_butm), '0'), 38, 10) is null and 
                    src:qbfd_butm is not null) THEN
                    'qbfd_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buvl), '0'), 38, 10) is null and 
                    src:qbfd_buvl is not null) THEN
                    'qbfd_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buwg), '0'), 38, 10) is null and 
                    src:qbfd_buwg is not null) THEN
                    'qbfd_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qbfd_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcmp), '0'), 38, 10) is null and 
                    src:qcmp is not null) THEN
                    'qcmp contains a non-numeric value : \'' || AS_VARCHAR(src:qcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpli), '0'), 38, 10) is null and 
                    src:qpli is not null) THEN
                    'qpli contains a non-numeric value : \'' || AS_VARCHAR(src:qpli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qplo), '0'), 38, 10) is null and 
                    src:qplo is not null) THEN
                    'qplo contains a non-numeric value : \'' || AS_VARCHAR(src:qplo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpnt), '0'), 38, 10) is null and 
                    src:qpnt is not null) THEN
                    'qpnt contains a non-numeric value : \'' || AS_VARCHAR(src:qpnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar), '0'), 38, 10) is null and 
                    src:qqar is not null) THEN
                    'qqar contains a non-numeric value : \'' || AS_VARCHAR(src:qqar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is null and 
                    src:qqar_buar is not null) THEN
                    'qqar_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is null and 
                    src:qqar_buln is not null) THEN
                    'qqar_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is null and 
                    src:qqar_bupc is not null) THEN
                    'qqar_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is null and 
                    src:qqar_butm is not null) THEN
                    'qqar_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is null and 
                    src:qqar_buvl is not null) THEN
                    'qqar_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is null and 
                    src:qqar_buwg is not null) THEN
                    'qqar_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc), '0'), 38, 10) is null and 
                    src:qrjc is not null) THEN
                    'qrjc contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is null and 
                    src:qrjc_buar is not null) THEN
                    'qrjc_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is null and 
                    src:qrjc_buln is not null) THEN
                    'qrjc_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is null and 
                    src:qrjc_bupc is not null) THEN
                    'qrjc_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is null and 
                    src:qrjc_butm is not null) THEN
                    'qrjc_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is null and 
                    src:qrjc_buvl is not null) THEN
                    'qrjc_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is null and 
                    src:qrjc_buwg is not null) THEN
                    'qrjc_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc), '0'), 38, 10) is null and 
                    src:qsbc is not null) THEN
                    'qsbc contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buar), '0'), 38, 10) is null and 
                    src:qsbc_buar is not null) THEN
                    'qsbc_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buln), '0'), 38, 10) is null and 
                    src:qsbc_buln is not null) THEN
                    'qsbc_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_bupc), '0'), 38, 10) is null and 
                    src:qsbc_bupc is not null) THEN
                    'qsbc_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_butm), '0'), 38, 10) is null and 
                    src:qsbc_butm is not null) THEN
                    'qsbc_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buvl), '0'), 38, 10) is null and 
                    src:qsbc_buvl is not null) THEN
                    'qsbc_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buwg), '0'), 38, 10) is null and 
                    src:qsbc_buwg is not null) THEN
                    'qsbc_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qsbc_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf), '0'), 38, 10) is null and 
                    src:qtbf is not null) THEN
                    'qtbf contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buar), '0'), 38, 10) is null and 
                    src:qtbf_buar is not null) THEN
                    'qtbf_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buln), '0'), 38, 10) is null and 
                    src:qtbf_buln is not null) THEN
                    'qtbf_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_bupc), '0'), 38, 10) is null and 
                    src:qtbf_bupc is not null) THEN
                    'qtbf_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_butm), '0'), 38, 10) is null and 
                    src:qtbf_butm is not null) THEN
                    'qtbf_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buvl), '0'), 38, 10) is null and 
                    src:qtbf_buvl is not null) THEN
                    'qtbf_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buwg), '0'), 38, 10) is null and 
                    src:qtbf_buwg is not null) THEN
                    'qtbf_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qtbf_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi), '0'), 38, 10) is null and 
                    src:qtbi is not null) THEN
                    'qtbi contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buar), '0'), 38, 10) is null and 
                    src:qtbi_buar is not null) THEN
                    'qtbi_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buln), '0'), 38, 10) is null and 
                    src:qtbi_buln is not null) THEN
                    'qtbi_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_bupc), '0'), 38, 10) is null and 
                    src:qtbi_bupc is not null) THEN
                    'qtbi_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_butm), '0'), 38, 10) is null and 
                    src:qtbi_butm is not null) THEN
                    'qtbi_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buvl), '0'), 38, 10) is null and 
                    src:qtbi_buvl is not null) THEN
                    'qtbi_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buwg), '0'), 38, 10) is null and 
                    src:qtbi_buwg is not null) THEN
                    'qtbi_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qtbi_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor), '0'), 38, 10) is null and 
                    src:qtor is not null) THEN
                    'qtor contains a non-numeric value : \'' || AS_VARCHAR(src:qtor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buar), '0'), 38, 10) is null and 
                    src:qtor_buar is not null) THEN
                    'qtor_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buln), '0'), 38, 10) is null and 
                    src:qtor_buln is not null) THEN
                    'qtor_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_bupc), '0'), 38, 10) is null and 
                    src:qtor_bupc is not null) THEN
                    'qtor_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_butm), '0'), 38, 10) is null and 
                    src:qtor_butm is not null) THEN
                    'qtor_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buvl), '0'), 38, 10) is null and 
                    src:qtor_buvl is not null) THEN
                    'qtor_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buwg), '0'), 38, 10) is null and 
                    src:qtor_buwg is not null) THEN
                    'qtor_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qtor_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtrt), '0'), 38, 10) is null and 
                    src:qtrt is not null) THEN
                    'qtrt contains a non-numeric value : \'' || AS_VARCHAR(src:qtrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) THEN
                    'qutm contains a non-numeric value : \'' || AS_VARCHAR(src:qutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qxac), '0'), 38, 10) is null and 
                    src:qxac is not null) THEN
                    'qxac contains a non-numeric value : \'' || AS_VARCHAR(src:qxac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco_ref_compnr), '0'), 38, 10) is null and 
                    src:reco_ref_compnr is not null) THEN
                    'reco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retm), '0'), 38, 10) is null and 
                    src:retm is not null) THEN
                    'retm contains a non-numeric value : \'' || AS_VARCHAR(src:retm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rmch), '0'), 38, 10) is null and 
                    src:rmch is not null) THEN
                    'rmch contains a non-numeric value : \'' || AS_VARCHAR(src:rmch) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rsdt), '1900-01-01')) is null and 
                    src:rsdt is not null) THEN
                    'rsdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rsdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) THEN
                    'runi contains a non-numeric value : \'' || AS_VARCHAR(src:runi) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rusd), '1900-01-01')) is null and 
                    src:rusd is not null) THEN
                    'rusd contains a non-timestamp value : \'' || AS_VARCHAR(src:rusd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rutm), '0'), 38, 10) is null and 
                    src:rutm is not null) THEN
                    'rutm contains a non-numeric value : \'' || AS_VARCHAR(src:rutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) THEN
                    'scpq contains a non-numeric value : \'' || AS_VARCHAR(src:scpq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is null and 
                    src:scpq_buar is not null) THEN
                    'scpq_buar contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is null and 
                    src:scpq_buln is not null) THEN
                    'scpq_buln contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is null and 
                    src:scpq_bupc is not null) THEN
                    'scpq_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is null and 
                    src:scpq_butm is not null) THEN
                    'scpq_butm contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is null and 
                    src:scpq_buvl is not null) THEN
                    'scpq_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is null and 
                    src:scpq_buwg is not null) THEN
                    'scpq_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sctt), '0'), 38, 10) is null and 
                    src:sctt is not null) THEN
                    'sctt contains a non-numeric value : \'' || AS_VARCHAR(src:sctt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdoc), '0'), 38, 10) is null and 
                    src:sdoc is not null) THEN
                    'sdoc contains a non-numeric value : \'' || AS_VARCHAR(src:sdoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltm), '0'), 38, 10) is null and 
                    src:sltm is not null) THEN
                    'sltm contains a non-numeric value : \'' || AS_VARCHAR(src:sltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sopr), '0'), 38, 10) is null and 
                    src:sopr is not null) THEN
                    'sopr contains a non-numeric value : \'' || AS_VARCHAR(src:sopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sptm), '0'), 38, 10) is null and 
                    src:sptm is not null) THEN
                    'sptm contains a non-numeric value : \'' || AS_VARCHAR(src:sptm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sset), '0'), 38, 10) is null and 
                    src:sset is not null) THEN
                    'sset contains a non-numeric value : \'' || AS_VARCHAR(src:sset) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssmd), '0'), 38, 10) is null and 
                    src:ssmd is not null) THEN
                    'ssmd contains a non-numeric value : \'' || AS_VARCHAR(src:ssmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sstm), '0'), 38, 10) is null and 
                    src:sstm is not null) THEN
                    'sstm contains a non-numeric value : \'' || AS_VARCHAR(src:sstm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suba_ref_compnr), '0'), 38, 10) is null and 
                    src:suba_ref_compnr is not null) THEN
                    'suba_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:suba_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subr), '0'), 38, 10) is null and 
                    src:subr is not null) THEN
                    'subr contains a non-numeric value : \'' || AS_VARCHAR(src:subr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is null and 
                    src:subs_ref_compnr is not null) THEN
                    'subs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:subs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutm), '0'), 38, 10) is null and 
                    src:sutm is not null) THEN
                    'sutm contains a non-numeric value : \'' || AS_VARCHAR(src:sutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tano_ref_compnr), '0'), 38, 10) is null and 
                    src:tano_ref_compnr is not null) THEN
                    'tano_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tano_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlyn), '0'), 38, 10) is null and 
                    src:tlyn is not null) THEN
                    'tlyn contains a non-numeric value : \'' || AS_VARCHAR(src:tlyn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdf), '1900-01-01')) is null and 
                    src:trdf is not null) THEN
                    'trdf contains a non-timestamp value : \'' || AS_VARCHAR(src:trdf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdl), '0'), 38, 10) is null and 
                    src:trdl is not null) THEN
                    'trdl contains a non-numeric value : \'' || AS_VARCHAR(src:trdl) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trls), '0'), 38, 10) is null and 
                    src:trls is not null) THEN
                    'trls contains a non-numeric value : \'' || AS_VARCHAR(src:trls) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trno), '1900-01-01')) is null and 
                    src:trno is not null) THEN
                    'trno contains a non-timestamp value : \'' || AS_VARCHAR(src:trno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuni), '0'), 38, 10) is null and 
                    src:tuni is not null) THEN
                    'tuni contains a non-numeric value : \'' || AS_VARCHAR(src:tuni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcst_cwoc_mtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:wcst_cwoc_mtyp_ref_compnr is not null) THEN
                    'wcst_cwoc_mtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wcst_cwoc_mtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whsa_ref_compnr), '0'), 38, 10) is null and 
                    src:whsa_ref_compnr is not null) THEN
                    'whsa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:whsa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ydtp), '0'), 38, 10) is null and 
                    src:ydtp is not null) THEN
                    'ydtp contains a non-numeric value : \'' || AS_VARCHAR(src:ydtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:yldp), '0'), 38, 10) is null and 
                    src:yldp is not null) THEN
                    'yldp contains a non-numeric value : \'' || AS_VARCHAR(src:yldp) || '\'' WHEN 
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
                src:pdno,
                src:opno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TISFC010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ardt), '1900-01-01')) is null and 
                    src:ardt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:asdt), '1900-01-01')) is null and 
                    src:asdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfls), '0'), 38, 10) is null and 
                    src:bfls is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blcd_ref_compnr), '0'), 38, 10) is null and 
                    src:blcd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctt), '0'), 38, 10) is null and 
                    src:cctt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copo), '0'), 38, 10) is null and 
                    src:copo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmso), '0'), 38, 10) is null and 
                    src:dmso is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:esdt), '1900-01-01')) is null and 
                    src:esdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdur), '0'), 38, 10) is null and 
                    src:fdur is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:fidt), '1900-01-01')) is null and 
                    src:fidt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxpd), '0'), 38, 10) is null and 
                    src:fxpd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fxsu), '0'), 38, 10) is null and 
                    src:fxsu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lfdt), '1900-01-01')) is null and 
                    src:lfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maho), '0'), 38, 10) is null and 
                    src:maho is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcho), '0'), 38, 10) is null and 
                    src:mcho is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcno_ref_compnr), '0'), 38, 10) is null and 
                    src:mcno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcoc), '0'), 38, 10) is null and 
                    src:mcoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnrs), '0'), 38, 10) is null and 
                    src:mnrs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopr), '0'), 38, 10) is null and 
                    src:mopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:most), '0'), 38, 10) is null and 
                    src:most is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:mtyp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:mvrd), '1900-01-01')) is null and 
                    src:mvrd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvtm), '0'), 38, 10) is null and 
                    src:mvtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nnts), '0'), 38, 10) is null and 
                    src:nnts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nomc), '0'), 38, 10) is null and 
                    src:nomc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nopr), '0'), 38, 10) is null and 
                    src:nopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oopn), '0'), 38, 10) is null and 
                    src:oopn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opno), '0'), 38, 10) is null and 
                    src:opno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opst), '0'), 38, 10) is null and 
                    src:opst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pfdt), '1900-01-01')) is null and 
                    src:pfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp), '0'), 38, 10) is null and 
                    src:phsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buar), '0'), 38, 10) is null and 
                    src:phsp_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buln), '0'), 38, 10) is null and 
                    src:phsp_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_bupc), '0'), 38, 10) is null and 
                    src:phsp_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_butm), '0'), 38, 10) is null and 
                    src:phsp_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buvl), '0'), 38, 10) is null and 
                    src:phsp_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phsp_buwg), '0'), 38, 10) is null and 
                    src:phsp_buwg is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prrd), '1900-01-01')) is null and 
                    src:prrd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prte), '0'), 38, 10) is null and 
                    src:prte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtm), '0'), 38, 10) is null and 
                    src:prtm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:psdt), '1900-01-01')) is null and 
                    src:psdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd), '0'), 38, 10) is null and 
                    src:qbfd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buar), '0'), 38, 10) is null and 
                    src:qbfd_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buln), '0'), 38, 10) is null and 
                    src:qbfd_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_bupc), '0'), 38, 10) is null and 
                    src:qbfd_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_butm), '0'), 38, 10) is null and 
                    src:qbfd_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buvl), '0'), 38, 10) is null and 
                    src:qbfd_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbfd_buwg), '0'), 38, 10) is null and 
                    src:qbfd_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcmp), '0'), 38, 10) is null and 
                    src:qcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpli), '0'), 38, 10) is null and 
                    src:qpli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qplo), '0'), 38, 10) is null and 
                    src:qplo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpnt), '0'), 38, 10) is null and 
                    src:qpnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar), '0'), 38, 10) is null and 
                    src:qqar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is null and 
                    src:qqar_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is null and 
                    src:qqar_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is null and 
                    src:qqar_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is null and 
                    src:qqar_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is null and 
                    src:qqar_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is null and 
                    src:qqar_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc), '0'), 38, 10) is null and 
                    src:qrjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is null and 
                    src:qrjc_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is null and 
                    src:qrjc_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is null and 
                    src:qrjc_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is null and 
                    src:qrjc_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is null and 
                    src:qrjc_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is null and 
                    src:qrjc_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc), '0'), 38, 10) is null and 
                    src:qsbc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buar), '0'), 38, 10) is null and 
                    src:qsbc_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buln), '0'), 38, 10) is null and 
                    src:qsbc_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_bupc), '0'), 38, 10) is null and 
                    src:qsbc_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_butm), '0'), 38, 10) is null and 
                    src:qsbc_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buvl), '0'), 38, 10) is null and 
                    src:qsbc_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qsbc_buwg), '0'), 38, 10) is null and 
                    src:qsbc_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf), '0'), 38, 10) is null and 
                    src:qtbf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buar), '0'), 38, 10) is null and 
                    src:qtbf_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buln), '0'), 38, 10) is null and 
                    src:qtbf_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_bupc), '0'), 38, 10) is null and 
                    src:qtbf_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_butm), '0'), 38, 10) is null and 
                    src:qtbf_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buvl), '0'), 38, 10) is null and 
                    src:qtbf_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbf_buwg), '0'), 38, 10) is null and 
                    src:qtbf_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi), '0'), 38, 10) is null and 
                    src:qtbi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buar), '0'), 38, 10) is null and 
                    src:qtbi_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buln), '0'), 38, 10) is null and 
                    src:qtbi_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_bupc), '0'), 38, 10) is null and 
                    src:qtbi_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_butm), '0'), 38, 10) is null and 
                    src:qtbi_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buvl), '0'), 38, 10) is null and 
                    src:qtbi_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtbi_buwg), '0'), 38, 10) is null and 
                    src:qtbi_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor), '0'), 38, 10) is null and 
                    src:qtor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buar), '0'), 38, 10) is null and 
                    src:qtor_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buln), '0'), 38, 10) is null and 
                    src:qtor_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_bupc), '0'), 38, 10) is null and 
                    src:qtor_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_butm), '0'), 38, 10) is null and 
                    src:qtor_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buvl), '0'), 38, 10) is null and 
                    src:qtor_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtor_buwg), '0'), 38, 10) is null and 
                    src:qtor_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtrt), '0'), 38, 10) is null and 
                    src:qtrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qxac), '0'), 38, 10) is null and 
                    src:qxac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco_ref_compnr), '0'), 38, 10) is null and 
                    src:reco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retm), '0'), 38, 10) is null and 
                    src:retm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rmch), '0'), 38, 10) is null and 
                    src:rmch is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rsdt), '1900-01-01')) is null and 
                    src:rsdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rusd), '1900-01-01')) is null and 
                    src:rusd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rutm), '0'), 38, 10) is null and 
                    src:rutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is null and 
                    src:scpq_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is null and 
                    src:scpq_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is null and 
                    src:scpq_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is null and 
                    src:scpq_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is null and 
                    src:scpq_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is null and 
                    src:scpq_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sctt), '0'), 38, 10) is null and 
                    src:sctt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdoc), '0'), 38, 10) is null and 
                    src:sdoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltm), '0'), 38, 10) is null and 
                    src:sltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sopr), '0'), 38, 10) is null and 
                    src:sopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sptm), '0'), 38, 10) is null and 
                    src:sptm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sset), '0'), 38, 10) is null and 
                    src:sset is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssmd), '0'), 38, 10) is null and 
                    src:ssmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sstm), '0'), 38, 10) is null and 
                    src:sstm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:suba_ref_compnr), '0'), 38, 10) is null and 
                    src:suba_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subr), '0'), 38, 10) is null and 
                    src:subr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is null and 
                    src:subs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutm), '0'), 38, 10) is null and 
                    src:sutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tano_ref_compnr), '0'), 38, 10) is null and 
                    src:tano_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlyn), '0'), 38, 10) is null and 
                    src:tlyn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdf), '1900-01-01')) is null and 
                    src:trdf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdl), '0'), 38, 10) is null and 
                    src:trdl is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trls), '0'), 38, 10) is null and 
                    src:trls is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trno), '1900-01-01')) is null and 
                    src:trno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuni), '0'), 38, 10) is null and 
                    src:tuni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcst_cwoc_mtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:wcst_cwoc_mtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whsa_ref_compnr), '0'), 38, 10) is null and 
                    src:whsa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ydtp), '0'), 38, 10) is null and 
                    src:ydtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:yldp), '0'), 38, 10) is null and 
                    src:yldp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)