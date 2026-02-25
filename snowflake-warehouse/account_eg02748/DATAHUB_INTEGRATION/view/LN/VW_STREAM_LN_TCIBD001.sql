CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCIBD001 AS SELECT
                        src:ccde::varchar AS ccde, 
                        src:ccde_ref_compnr::integer AS ccde_ref_compnr, 
                        src:ccfg::varchar AS ccfg, 
                        src:ccfg_ref_compnr::integer AS ccfg_ref_compnr, 
                        src:cean::varchar AS cean, 
                        src:chma::integer AS chma, 
                        src:chma_kw::varchar AS chma_kw, 
                        src:citg::varchar AS citg, 
                        src:citg_ref_compnr::integer AS citg_ref_compnr, 
                        src:cltr::integer AS cltr, 
                        src:cltr_kw::varchar AS cltr_kw, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:cnfg::integer AS cnfg, 
                        src:cnfg_kw::varchar AS cnfg_kw, 
                        src:cntr::varchar AS cntr, 
                        src:cntr_ref_compnr::integer AS cntr_ref_compnr, 
                        src:coeu::integer AS coeu, 
                        src:coeu_kw::varchar AS coeu_kw, 
                        src:compnr::integer AS compnr, 
                        src:cont::integer AS cont, 
                        src:cont_kw::varchar AS cont_kw, 
                        src:cood::varchar AS cood, 
                        src:cood_ref_compnr::integer AS cood_ref_compnr, 
                        src:cpcl::varchar AS cpcl, 
                        src:cpcl_ref_compnr::integer AS cpcl_ref_compnr, 
                        src:cpcp::varchar AS cpcp, 
                        src:cpcp_ref_compnr::integer AS cpcp_ref_compnr, 
                        src:cpln::varchar AS cpln, 
                        src:cpln_ref_compnr::integer AS cpln_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cpva::integer AS cpva, 
                        src:cpva_rcmp::integer AS cpva_rcmp, 
                        src:crdt::datetime AS crdt, 
                        src:csel::varchar AS csel, 
                        src:csel_ref_compnr::integer AS csel_ref_compnr, 
                        src:csig::varchar AS csig, 
                        src:csig_ref_compnr::integer AS csig_ref_compnr, 
                        src:ctyo::varchar AS ctyo, 
                        src:ctyo_ref_compnr::integer AS ctyo_ref_compnr, 
                        src:ctyp::varchar AS ctyp, 
                        src:ctyp_ref_compnr::integer AS ctyp_ref_compnr, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cust::integer AS cust, 
                        src:cust_kw::varchar AS cust_kw, 
                        src:cwun::varchar AS cwun, 
                        src:cwun_ref_compnr::integer AS cwun_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dfit::varchar AS dfit, 
                        src:dfit_ref_compnr::integer AS dfit_ref_compnr, 
                        src:dpcr::integer AS dpcr, 
                        src:dpcr_kw::varchar AS dpcr_kw, 
                        src:dpeg::integer AS dpeg, 
                        src:dpeg_kw::varchar AS dpeg_kw, 
                        src:dptp::integer AS dptp, 
                        src:dptp_kw::varchar AS dptp_kw, 
                        src:dpuu::integer AS dpuu, 
                        src:dpuu_kw::varchar AS dpuu_kw, 
                        src:dsca::object AS dsca, 
                        src:dscb::object AS dscb, 
                        src:dscc::object AS dscc, 
                        src:dscd::object AS dscd, 
                        src:dsct::integer AS dsct, 
                        src:dsct_kw::varchar AS dsct_kw, 
                        src:edco::integer AS edco, 
                        src:edco_kw::varchar AS edco_kw, 
                        src:efco::varchar AS efco, 
                        src:efpr::integer AS efpr, 
                        src:efpr_kw::varchar AS efpr_kw, 
                        src:eitm::integer AS eitm, 
                        src:eitm_kw::varchar AS eitm_kw, 
                        src:elcm::integer AS elcm, 
                        src:elcm_kw::varchar AS elcm_kw, 
                        src:elrq::integer AS elrq, 
                        src:elrq_kw::varchar AS elrq_kw, 
                        src:envc::integer AS envc, 
                        src:envc_kw::varchar AS envc_kw, 
                        src:exin::varchar AS exin, 
                        src:fstk::integer AS fstk, 
                        src:fstk_kw::varchar AS fstk_kw, 
                        src:ichg::integer AS ichg, 
                        src:ichg_kw::varchar AS ichg_kw, 
                        src:icsi::integer AS icsi, 
                        src:icsi_kw::varchar AS icsi_kw, 
                        src:imag::varchar AS imag, 
                        src:indt::datetime AS indt, 
                        src:ippg::integer AS ippg, 
                        src:ippg_kw::varchar AS ippg_kw, 
                        src:item::varchar AS item, 
                        src:itmt::integer AS itmt, 
                        src:itmt_kw::varchar AS itmt_kw, 
                        src:kitm::integer AS kitm, 
                        src:kitm_kw::varchar AS kitm_kw, 
                        src:lctr::integer AS lctr, 
                        src:lctr_kw::varchar AS lctr_kw, 
                        src:lmdt::datetime AS lmdt, 
                        src:ltct::integer AS ltct, 
                        src:ltct_kw::varchar AS ltct_kw, 
                        src:mcoa::integer AS mcoa, 
                        src:mcoa_kw::varchar AS mcoa_kw, 
                        src:oktm::integer AS oktm, 
                        src:opol::integer AS opol, 
                        src:opol_kw::varchar AS opol_kw, 
                        src:opts::integer AS opts, 
                        src:osys::integer AS osys, 
                        src:osys_kw::varchar AS osys_kw, 
                        src:ppeg::integer AS ppeg, 
                        src:ppeg_kw::varchar AS ppeg_kw, 
                        src:ppss::integer AS ppss, 
                        src:ppss_kw::varchar AS ppss_kw, 
                        src:psiu::integer AS psiu, 
                        src:psiu_kw::varchar AS psiu_kw, 
                        src:rdpt::varchar AS rdpt, 
                        src:rdpt_ref_compnr::integer AS rdpt_ref_compnr, 
                        src:repl::integer AS repl, 
                        src:repl_kw::varchar AS repl_kw, 
                        src:rnum::integer AS rnum, 
                        src:rnum_kw::varchar AS rnum_kw, 
                        src:rvdi::integer AS rvdi, 
                        src:rvdi_kw::varchar AS rvdi_kw, 
                        src:sayn::integer AS sayn, 
                        src:sayn_kw::varchar AS sayn_kw, 
                        src:seab::object AS seab, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:seri::integer AS seri, 
                        src:seri_kw::varchar AS seri_kw, 
                        src:sgtc::integer AS sgtc, 
                        src:sgtc_kw::varchar AS sgtc_kw, 
                        src:srce::integer AS srce, 
                        src:srce_kw::varchar AS srce_kw, 
                        src:stoi::integer AS stoi, 
                        src:stoi_kw::varchar AS stoi_kw, 
                        src:styp::integer AS styp, 
                        src:styp_kw::varchar AS styp_kw, 
                        src:subc::integer AS subc, 
                        src:subc_kw::varchar AS subc_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:uefs::integer AS uefs, 
                        src:uefs_kw::varchar AS uefs_kw, 
                        src:umer::integer AS umer, 
                        src:umer_kw::varchar AS umer_kw, 
                        src:unef::integer AS unef, 
                        src:unef_kw::varchar AS unef_kw, 
                        src:username::varchar AS username, 
                        src:uset::varchar AS uset, 
                        src:uset_cwun_ref_compnr::integer AS uset_cwun_ref_compnr, 
                        src:uset_ref_compnr::integer AS uset_ref_compnr, 
                        src:wght::numeric(38, 17) AS wght, 
                        src:wpcs::integer AS wpcs, 
                        src:wpcs_kw::varchar AS wpcs_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:compnr,
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCIBD001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccde_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccfg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cltr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnfg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cntr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coeu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cont), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cood_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpva), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpva_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csel_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csig_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cust), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwun_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfit_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dpcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dpeg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dptp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dpuu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:edco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:efpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eitm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elcm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:envc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstk), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ichg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icsi), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:indt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ippg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kitm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lctr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcoa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oktm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opol), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osys), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppeg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psiu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:repl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rnum), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rvdi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sayn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sgtc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srce), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stoi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:styp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uefs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:umer), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unef), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uset_cwun_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uset_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wght), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wpcs), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null