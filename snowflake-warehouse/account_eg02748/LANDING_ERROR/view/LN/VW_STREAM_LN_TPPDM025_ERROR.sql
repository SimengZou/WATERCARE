CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM025_ERROR AS SELECT src, 'LN_TPPDM025' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) THEN
                    'avtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:avtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccr_ref_compnr), '0'), 38, 10) is null and 
                    src:cccr_ref_compnr is not null) THEN
                    'cccr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cccr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is null and 
                    src:ccde_ref_compnr is not null) THEN
                    'ccde_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccde_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) THEN
                    'ccfu contains a non-numeric value : \'' || AS_VARCHAR(src:ccfu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsr_ref_compnr is not null) THEN
                    'ccsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cequ_rcmp), '0'), 38, 10) is null and 
                    src:cequ_rcmp is not null) THEN
                    'cequ_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cequ_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) THEN
                    'citg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is null and 
                    src:cood_ref_compnr is not null) THEN
                    'cood_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cood_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcl_ref_compnr is not null) THEN
                    'cpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) THEN
                    'cpgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) THEN
                    'cpgs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is null and 
                    src:cpln_ref_compnr is not null) THEN
                    'cpln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpln_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) THEN
                    'ctyo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) THEN
                    'ctyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) THEN
                    'cwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcpu), '0'), 38, 10) is null and 
                    src:dcpu is not null) THEN
                    'dcpu contains a non-numeric value : \'' || AS_VARCHAR(src:dcpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsbc), '0'), 38, 10) is null and 
                    src:dsbc is not null) THEN
                    'dsbc contains a non-numeric value : \'' || AS_VARCHAR(src:dsbc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:envc), '0'), 38, 10) is null and 
                    src:envc is not null) THEN
                    'envc contains a non-numeric value : \'' || AS_VARCHAR(src:envc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exeq), '0'), 38, 10) is null and 
                    src:exeq is not null) THEN
                    'exeq contains a non-numeric value : \'' || AS_VARCHAR(src:exeq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icsi), '0'), 38, 10) is null and 
                    src:icsi is not null) THEN
                    'icsi contains a non-numeric value : \'' || AS_VARCHAR(src:icsi) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrc), '1900-01-01')) is null and 
                    src:ltrc is not null) THEN
                    'ltrc contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrp), '1900-01-01')) is null and 
                    src:ltrp is not null) THEN
                    'ltrp contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrs), '1900-01-01')) is null and 
                    src:ltrs is not null) THEN
                    'ltrs contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mequ), '0'), 38, 10) is null and 
                    src:mequ is not null) THEN
                    'mequ contains a non-numeric value : \'' || AS_VARCHAR(src:mequ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) THEN
                    'oltm contains a non-numeric value : \'' || AS_VARCHAR(src:oltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) THEN
                    'osys contains a non-numeric value : \'' || AS_VARCHAR(src:osys) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pleq_ref_compnr), '0'), 38, 10) is null and 
                    src:pleq_ref_compnr is not null) THEN
                    'pleq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pleq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prby), '0'), 38, 10) is null and 
                    src:prby is not null) THEN
                    'prby contains a non-numeric value : \'' || AS_VARCHAR(src:prby) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) THEN
                    'prre contains a non-numeric value : \'' || AS_VARCHAR(src:prre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psiu), '0'), 38, 10) is null and 
                    src:psiu is not null) THEN
                    'psiu contains a non-numeric value : \'' || AS_VARCHAR(src:psiu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) THEN
                    'ratc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp), '0'), 38, 10) is null and 
                    src:ratp is not null) THEN
                    'ratp contains a non-numeric value : \'' || AS_VARCHAR(src:ratp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) THEN
                    'rats contains a non-numeric value : \'' || AS_VARCHAR(src:rats) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is null and 
                    src:rcmc_1 is not null) THEN
                    'rcmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is null and 
                    src:rcmc_2 is not null) THEN
                    'rcmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is null and 
                    src:rcmc_3 is not null) THEN
                    'rcmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rdpt_ref_compnr is not null) THEN
                    'rdpt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rdpt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpmc_1), '0'), 38, 10) is null and 
                    src:rpmc_1 is not null) THEN
                    'rpmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rpmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpmc_2), '0'), 38, 10) is null and 
                    src:rpmc_2 is not null) THEN
                    'rpmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rpmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpmc_3), '0'), 38, 10) is null and 
                    src:rpmc_3 is not null) THEN
                    'rpmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rpmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is null and 
                    src:rsmc_1 is not null) THEN
                    'rsmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is null and 
                    src:rsmc_2 is not null) THEN
                    'rsmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is null and 
                    src:rsmc_3 is not null) THEN
                    'rsmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seri), '0'), 38, 10) is null and 
                    src:seri is not null) THEN
                    'seri contains a non-numeric value : \'' || AS_VARCHAR(src:seri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sprp), '0'), 38, 10) is null and 
                    src:sprp is not null) THEN
                    'sprp contains a non-numeric value : \'' || AS_VARCHAR(src:sprp) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unav), '0'), 38, 10) is null and 
                    src:unav is not null) THEN
                    'unav contains a non-numeric value : \'' || AS_VARCHAR(src:unav) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_cuni_ref_compnr is not null) THEN
                    'uset_cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) THEN
                    'uset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usyn), '0'), 38, 10) is null and 
                    src:usyn is not null) THEN
                    'usyn contains a non-numeric value : \'' || AS_VARCHAR(src:usyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
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
                                    
                src:cequ,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM025 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is null and 
                    src:avtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccr_ref_compnr), '0'), 38, 10) is null and 
                    src:cccr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is null and 
                    src:ccde_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cequ_rcmp), '0'), 38, 10) is null and 
                    src:cequ_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is null and 
                    src:cood_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is null and 
                    src:cpln_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ctyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is null and 
                    src:cwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcpu), '0'), 38, 10) is null and 
                    src:dcpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsbc), '0'), 38, 10) is null and 
                    src:dsbc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:envc), '0'), 38, 10) is null and 
                    src:envc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exeq), '0'), 38, 10) is null and 
                    src:exeq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icsi), '0'), 38, 10) is null and 
                    src:icsi is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrc), '1900-01-01')) is null and 
                    src:ltrc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrp), '1900-01-01')) is null and 
                    src:ltrp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrs), '1900-01-01')) is null and 
                    src:ltrs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mequ), '0'), 38, 10) is null and 
                    src:mequ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osys), '0'), 38, 10) is null and 
                    src:osys is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pleq_ref_compnr), '0'), 38, 10) is null and 
                    src:pleq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prby), '0'), 38, 10) is null and 
                    src:prby is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psiu), '0'), 38, 10) is null and 
                    src:psiu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratp), '0'), 38, 10) is null and 
                    src:ratp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is null and 
                    src:rcmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is null and 
                    src:rcmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is null and 
                    src:rcmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is null and 
                    src:rdpt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpmc_1), '0'), 38, 10) is null and 
                    src:rpmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpmc_2), '0'), 38, 10) is null and 
                    src:rpmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpmc_3), '0'), 38, 10) is null and 
                    src:rpmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is null and 
                    src:rsmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is null and 
                    src:rsmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is null and 
                    src:rsmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seri), '0'), 38, 10) is null and 
                    src:seri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sprp), '0'), 38, 10) is null and 
                    src:sprp is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unav), '0'), 38, 10) is null and 
                    src:unav is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is null and 
                    src:uset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usyn), '0'), 38, 10) is null and 
                    src:usyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)