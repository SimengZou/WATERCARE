CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM035_ERROR AS SELECT src, 'LN_TPPDM035' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buni), '0'), 38, 10) is null and 
                    src:buni is not null) THEN
                    'buni contains a non-numeric value : \'' || AS_VARCHAR(src:buni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccp_ref_compnr), '0'), 38, 10) is null and 
                    src:cccp_ref_compnr is not null) THEN
                    'cccp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cccp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is null and 
                    src:ccde_ref_compnr is not null) THEN
                    'ccde_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccde_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) THEN
                    'ccfu contains a non-numeric value : \'' || AS_VARCHAR(src:ccfu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsp_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsp_ref_compnr is not null) THEN
                    'ccsp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccsp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) THEN
                    'citg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is null and 
                    src:cood_ref_compnr is not null) THEN
                    'cood_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cood_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) THEN
                    'cpgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) THEN
                    'cpgs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cppp), '0'), 38, 10) is null and 
                    src:cppp is not null) THEN
                    'cppp contains a non-numeric value : \'' || AS_VARCHAR(src:cppp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csel_ref_compnr), '0'), 38, 10) is null and 
                    src:csel_ref_compnr is not null) THEN
                    'csel_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csel_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is null and 
                    src:csgp_ref_compnr is not null) THEN
                    'csgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgs_ref_compnr), '0'), 38, 10) is null and 
                    src:csgs_ref_compnr is not null) THEN
                    'csgs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csig_ref_compnr), '0'), 38, 10) is null and 
                    src:csig_ref_compnr is not null) THEN
                    'csig_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csig_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csub_rcmp), '0'), 38, 10) is null and 
                    src:csub_rcmp is not null) THEN
                    'csub_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:csub_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsbc), '0'), 38, 10) is null and 
                    src:dsbc is not null) THEN
                    'dsbc contains a non-numeric value : \'' || AS_VARCHAR(src:dsbc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpc), '1900-01-01')) is null and 
                    src:ltpc is not null) THEN
                    'ltpc contains a non-timestamp value : \'' || AS_VARCHAR(src:ltpc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpp), '1900-01-01')) is null and 
                    src:ltpp is not null) THEN
                    'ltpp contains a non-timestamp value : \'' || AS_VARCHAR(src:ltpp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltps), '1900-01-01')) is null and 
                    src:ltps is not null) THEN
                    'ltps contains a non-timestamp value : \'' || AS_VARCHAR(src:ltps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) THEN
                    'oltm contains a non-numeric value : \'' || AS_VARCHAR(src:oltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) THEN
                    'osys contains a non-numeric value : \'' || AS_VARCHAR(src:osys) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_1), '0'), 38, 10) is null and 
                    src:pcmc_1 is not null) THEN
                    'pcmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:pcmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_2), '0'), 38, 10) is null and 
                    src:pcmc_2 is not null) THEN
                    'pcmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:pcmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_3), '0'), 38, 10) is null and 
                    src:pcmc_3 is not null) THEN
                    'pcmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:pcmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is null and 
                    src:plid_ref_compnr is not null) THEN
                    'plid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppmc_1), '0'), 38, 10) is null and 
                    src:ppmc_1 is not null) THEN
                    'ppmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ppmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppmc_2), '0'), 38, 10) is null and 
                    src:ppmc_2 is not null) THEN
                    'ppmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ppmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppmc_3), '0'), 38, 10) is null and 
                    src:ppmc_3 is not null) THEN
                    'ppmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ppmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prby), '0'), 38, 10) is null and 
                    src:prby is not null) THEN
                    'prby contains a non-numeric value : \'' || AS_VARCHAR(src:prby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prip), '0'), 38, 10) is null and 
                    src:prip is not null) THEN
                    'prip contains a non-numeric value : \'' || AS_VARCHAR(src:prip) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) THEN
                    'prre contains a non-numeric value : \'' || AS_VARCHAR(src:prre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psiu), '0'), 38, 10) is null and 
                    src:psiu is not null) THEN
                    'psiu contains a non-numeric value : \'' || AS_VARCHAR(src:psiu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_1), '0'), 38, 10) is null and 
                    src:psmc_1 is not null) THEN
                    'psmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:psmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_2), '0'), 38, 10) is null and 
                    src:psmc_2 is not null) THEN
                    'psmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:psmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_3), '0'), 38, 10) is null and 
                    src:psmc_3 is not null) THEN
                    'psmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:psmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwgc), '0'), 38, 10) is null and 
                    src:pwgc is not null) THEN
                    'pwgc contains a non-numeric value : \'' || AS_VARCHAR(src:pwgc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rdpt_ref_compnr is not null) THEN
                    'rdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtr_ref_compnr), '0'), 38, 10) is null and 
                    src:srtr_ref_compnr is not null) THEN
                    'srtr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:srtr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svat_ref_compnr), '0'), 38, 10) is null and 
                    src:svat_ref_compnr is not null) THEN
                    'svat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:svat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp), '0'), 38, 10) is null and 
                    src:txtp is not null) THEN
                    'txtp contains a non-numeric value : \'' || AS_VARCHAR(src:txtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp_ref_compnr), '0'), 38, 10) is null and 
                    src:txtp_ref_compnr is not null) THEN
                    'txtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts), '0'), 38, 10) is null and 
                    src:txts is not null) THEN
                    'txts contains a non-numeric value : \'' || AS_VARCHAR(src:txts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts_ref_compnr), '0'), 38, 10) is null and 
                    src:txts_ref_compnr is not null) THEN
                    'txts_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txts_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_cuni_ref_compnr is not null) THEN
                    'uset_cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) THEN
                    'uset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usyn), '0'), 38, 10) is null and 
                    src:usyn is not null) THEN
                    'usyn contains a non-numeric value : \'' || AS_VARCHAR(src:usyn) || '\'' WHEN 
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
                                    
                src:csub,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM035 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buni), '0'), 38, 10) is null and 
                    src:buni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccp_ref_compnr), '0'), 38, 10) is null and 
                    src:cccp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is null and 
                    src:ccde_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsp_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is null and 
                    src:cood_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cppp), '0'), 38, 10) is null and 
                    src:cppp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csel_ref_compnr), '0'), 38, 10) is null and 
                    src:csel_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is null and 
                    src:csgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgs_ref_compnr), '0'), 38, 10) is null and 
                    src:csgs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csig_ref_compnr), '0'), 38, 10) is null and 
                    src:csig_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csub_rcmp), '0'), 38, 10) is null and 
                    src:csub_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsbc), '0'), 38, 10) is null and 
                    src:dsbc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpc), '1900-01-01')) is null and 
                    src:ltpc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpp), '1900-01-01')) is null and 
                    src:ltpp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltps), '1900-01-01')) is null and 
                    src:ltps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_1), '0'), 38, 10) is null and 
                    src:pcmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_2), '0'), 38, 10) is null and 
                    src:pcmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_3), '0'), 38, 10) is null and 
                    src:pcmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plid_ref_compnr), '0'), 38, 10) is null and 
                    src:plid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppmc_1), '0'), 38, 10) is null and 
                    src:ppmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppmc_2), '0'), 38, 10) is null and 
                    src:ppmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppmc_3), '0'), 38, 10) is null and 
                    src:ppmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prby), '0'), 38, 10) is null and 
                    src:prby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prip), '0'), 38, 10) is null and 
                    src:prip is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psiu), '0'), 38, 10) is null and 
                    src:psiu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_1), '0'), 38, 10) is null and 
                    src:psmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_2), '0'), 38, 10) is null and 
                    src:psmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_3), '0'), 38, 10) is null and 
                    src:psmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwgc), '0'), 38, 10) is null and 
                    src:pwgc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtr_ref_compnr), '0'), 38, 10) is null and 
                    src:srtr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svat_ref_compnr), '0'), 38, 10) is null and 
                    src:svat_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp), '0'), 38, 10) is null and 
                    src:txtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtp_ref_compnr), '0'), 38, 10) is null and 
                    src:txtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts), '0'), 38, 10) is null and 
                    src:txts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts_ref_compnr), '0'), 38, 10) is null and 
                    src:txts_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usyn), '0'), 38, 10) is null and 
                    src:usyn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)