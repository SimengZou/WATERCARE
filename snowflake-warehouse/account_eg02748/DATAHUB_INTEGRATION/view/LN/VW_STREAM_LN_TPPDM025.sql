CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM025 AS SELECT
                        src:avtp::varchar AS avtp, 
                        src:avtp_ref_compnr::integer AS avtp_ref_compnr, 
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:cccr::varchar AS cccr, 
                        src:cccr_ref_compnr::integer AS cccr_ref_compnr, 
                        src:ccde::varchar AS ccde, 
                        src:ccde_ref_compnr::integer AS ccde_ref_compnr, 
                        src:cceq::varchar AS cceq, 
                        src:ccfu::integer AS ccfu, 
                        src:ccfu_kw::varchar AS ccfu_kw, 
                        src:ccsr::varchar AS ccsr, 
                        src:ccsr_ref_compnr::integer AS ccsr_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cean::varchar AS cean, 
                        src:ceqg::varchar AS ceqg, 
                        src:cequ::varchar AS cequ, 
                        src:cequ_rcmp::integer AS cequ_rcmp, 
                        src:citg::varchar AS citg, 
                        src:citg_ref_compnr::integer AS citg_ref_compnr, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cood::varchar AS cood, 
                        src:cood_ref_compnr::integer AS cood_ref_compnr, 
                        src:cpcl::varchar AS cpcl, 
                        src:cpcl_ref_compnr::integer AS cpcl_ref_compnr, 
                        src:cpgp::varchar AS cpgp, 
                        src:cpgp_ref_compnr::integer AS cpgp_ref_compnr, 
                        src:cpgs::varchar AS cpgs, 
                        src:cpgs_ref_compnr::integer AS cpgs_ref_compnr, 
                        src:cpln::varchar AS cpln, 
                        src:cpln_ref_compnr::integer AS cpln_ref_compnr, 
                        src:cppp::integer AS cppp, 
                        src:cppp_kw::varchar AS cppp_kw, 
                        src:csel::varchar AS csel, 
                        src:csel_ref_compnr::integer AS csel_ref_compnr, 
                        src:csgp::varchar AS csgp, 
                        src:csgp_ref_compnr::integer AS csgp_ref_compnr, 
                        src:csgs::varchar AS csgs, 
                        src:csgs_ref_compnr::integer AS csgs_ref_compnr, 
                        src:csig::varchar AS csig, 
                        src:csig_ref_compnr::integer AS csig_ref_compnr, 
                        src:ctyo::varchar AS ctyo, 
                        src:ctyo_ref_compnr::integer AS ctyo_ref_compnr, 
                        src:ctyp::varchar AS ctyp, 
                        src:ctyp_ref_compnr::integer AS ctyp_ref_compnr, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:cwun::varchar AS cwun, 
                        src:cwun_ref_compnr::integer AS cwun_ref_compnr, 
                        src:dcpu::numeric(38, 17) AS dcpu, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dsbc::integer AS dsbc, 
                        src:dsbc_kw::varchar AS dsbc_kw, 
                        src:dscb::object AS dscb, 
                        src:dscc::object AS dscc, 
                        src:dscd::object AS dscd, 
                        src:envc::integer AS envc, 
                        src:envc_kw::varchar AS envc_kw, 
                        src:exeq::integer AS exeq, 
                        src:exeq_kw::varchar AS exeq_kw, 
                        src:exin::varchar AS exin, 
                        src:icsi::integer AS icsi, 
                        src:icsi_kw::varchar AS icsi_kw, 
                        src:imag::varchar AS imag, 
                        src:ltrc::datetime AS ltrc, 
                        src:ltrp::datetime AS ltrp, 
                        src:ltrs::datetime AS ltrs, 
                        src:mequ::integer AS mequ, 
                        src:mequ_kw::varchar AS mequ_kw, 
                        src:oltm::integer AS oltm, 
                        src:osys::integer AS osys, 
                        src:osys_kw::varchar AS osys_kw, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:pleq::varchar AS pleq, 
                        src:pleq_ref_compnr::integer AS pleq_ref_compnr, 
                        src:prby::integer AS prby, 
                        src:prby_kw::varchar AS prby_kw, 
                        src:prre::integer AS prre, 
                        src:prre_kw::varchar AS prre_kw, 
                        src:psiu::integer AS psiu, 
                        src:psiu_kw::varchar AS psiu_kw, 
                        src:ratc::numeric(38, 17) AS ratc, 
                        src:ratp::numeric(38, 17) AS ratp, 
                        src:rats::numeric(38, 17) AS rats, 
                        src:rcmc_1::numeric(38, 17) AS rcmc_1, 
                        src:rcmc_2::numeric(38, 17) AS rcmc_2, 
                        src:rcmc_3::numeric(38, 17) AS rcmc_3, 
                        src:rdpt::varchar AS rdpt, 
                        src:rdpt_ref_compnr::integer AS rdpt_ref_compnr, 
                        src:rpmc_1::numeric(38, 17) AS rpmc_1, 
                        src:rpmc_2::numeric(38, 17) AS rpmc_2, 
                        src:rpmc_3::numeric(38, 17) AS rpmc_3, 
                        src:rsmc_1::numeric(38, 17) AS rsmc_1, 
                        src:rsmc_2::numeric(38, 17) AS rsmc_2, 
                        src:rsmc_3::numeric(38, 17) AS rsmc_3, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:seri::integer AS seri, 
                        src:seri_kw::varchar AS seri_kw, 
                        src:sprp::integer AS sprp, 
                        src:sprp_kw::varchar AS sprp_kw, 
                        src:srtr::varchar AS srtr, 
                        src:srtr_ref_compnr::integer AS srtr_ref_compnr, 
                        src:svat::varchar AS svat, 
                        src:svat_ref_compnr::integer AS svat_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:txtp::integer AS txtp, 
                        src:txtp_ref_compnr::integer AS txtp_ref_compnr, 
                        src:txts::integer AS txts, 
                        src:txts_ref_compnr::integer AS txts_ref_compnr, 
                        src:unav::numeric(38, 17) AS unav, 
                        src:username::varchar AS username, 
                        src:uset::varchar AS uset, 
                        src:uset_cuni_ref_compnr::integer AS uset_cuni_ref_compnr, 
                        src:uset_ref_compnr::integer AS uset_ref_compnr, 
                        src:usyn::integer AS usyn, 
                        src:usyn_kw::varchar AS usyn_kw, 
                        src:wght::numeric(38, 17) AS wght, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:avtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cccr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccfu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccsr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cequ_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cppp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csel_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csgs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csig_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dcpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsbc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:envc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exeq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icsi), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltrc), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltrp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltrs), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mequ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oltm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osys), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pleq_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prby), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prre), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psiu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rats), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sprp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srtr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:svat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txts_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unav), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uset_cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:usyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wght), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null