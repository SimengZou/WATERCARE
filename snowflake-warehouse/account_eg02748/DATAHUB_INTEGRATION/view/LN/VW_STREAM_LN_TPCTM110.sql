CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPCTM110 AS SELECT
                        src:acpn::integer AS acpn, 
                        src:acpn_kw::varchar AS acpn_kw, 
                        src:adin::varchar AS adin, 
                        src:adtp::integer AS adtp, 
                        src:adtp_kw::varchar AS adtp_kw, 
                        src:advn::integer AS advn, 
                        src:advn_kw::varchar AS advn_kw, 
                        src:bank::varchar AS bank, 
                        src:bcrp::integer AS bcrp, 
                        src:bcrp_kw::varchar AS bcrp_kw, 
                        src:bgrb::integer AS bgrb, 
                        src:bgrb_kw::varchar AS bgrb_kw, 
                        src:bgrq::integer AS bgrq, 
                        src:bgrq_kw::varchar AS bgrq_kw, 
                        src:blck::integer AS blck, 
                        src:blck_kw::varchar AS blck_kw, 
                        src:blcl::varchar AS blcl, 
                        src:blcl_ref_compnr::integer AS blcl_ref_compnr, 
                        src:bnum::varchar AS bnum, 
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:bver::varchar AS bver, 
                        src:cban::varchar AS cban, 
                        src:cbrn::varchar AS cbrn, 
                        src:cbrn_ref_compnr::integer AS cbrn_ref_compnr, 
                        src:ccam::varchar AS ccam, 
                        src:ccam_ref_compnr::integer AS ccam_ref_compnr, 
                        src:ccat::varchar AS ccat, 
                        src:ccat_ref_compnr::integer AS ccat_ref_compnr, 
                        src:ccot::varchar AS ccot, 
                        src:ccot_ref_compnr::integer AS ccot_ref_compnr, 
                        src:ccrs::varchar AS ccrs, 
                        src:ccrs_ref_compnr::integer AS ccrs_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:ceil::integer AS ceil, 
                        src:ceil_kw::varchar AS ceil_kw, 
                        src:ceno::varchar AS ceno, 
                        src:cfac::varchar AS cfac, 
                        src:cfac_ref_compnr::integer AS cfac_ref_compnr, 
                        src:cfrw::varchar AS cfrw, 
                        src:cfrw_ref_compnr::integer AS cfrw_ref_compnr, 
                        src:cidm::varchar AS cidm, 
                        src:cidm_ref_compnr::integer AS cidm_ref_compnr, 
                        src:cinm::varchar AS cinm, 
                        src:cinm_ref_compnr::integer AS cinm_ref_compnr, 
                        src:clam::numeric(38, 17) AS clam, 
                        src:clam_cntc::numeric(38, 17) AS clam_cntc, 
                        src:clam_dwhc::numeric(38, 17) AS clam_dwhc, 
                        src:clam_homc::numeric(38, 17) AS clam_homc, 
                        src:clam_prjc::numeric(38, 17) AS clam_prjc, 
                        src:clam_refc::numeric(38, 17) AS clam_refc, 
                        src:clam_rpc1::numeric(38, 17) AS clam_rpc1, 
                        src:clam_rpc2::numeric(38, 17) AS clam_rpc2, 
                        src:cmng::varchar AS cmng, 
                        src:cmng_ref_compnr::integer AS cmng_ref_compnr, 
                        src:cnln::varchar AS cnln, 
                        src:cnln_fcmp::integer AS cnln_fcmp, 
                        src:codt::datetime AS codt, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:copr::numeric(38, 17) AS copr, 
                        src:copr_cntc::numeric(38, 17) AS copr_cntc, 
                        src:copr_dwhc::numeric(38, 17) AS copr_dwhc, 
                        src:copr_homc::numeric(38, 17) AS copr_homc, 
                        src:copr_prjc::numeric(38, 17) AS copr_prjc, 
                        src:copr_refc::numeric(38, 17) AS copr_refc, 
                        src:copr_rpc1::numeric(38, 17) AS copr_rpc1, 
                        src:copr_rpc2::numeric(38, 17) AS copr_rpc2, 
                        src:coty::integer AS coty, 
                        src:coty_kw::varchar AS coty_kw, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_bver_bnum_ref_compnr::integer AS cprj_bver_bnum_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crby::varchar AS crby, 
                        src:crdt::datetime AS crdt, 
                        src:creg::varchar AS creg, 
                        src:creg_ref_compnr::integer AS creg_ref_compnr, 
                        src:crep::varchar AS crep, 
                        src:crep_ref_compnr::integer AS crep_ref_compnr, 
                        src:csec::varchar AS csec, 
                        src:csec_ref_compnr::integer AS csec_ref_compnr, 
                        src:cstg::varchar AS cstg, 
                        src:cstg_ref_compnr::integer AS cstg_ref_compnr, 
                        src:csts::integer AS csts, 
                        src:csts_kw::varchar AS csts_kw, 
                        src:ctyp::integer AS ctyp, 
                        src:ctyp_kw::varchar AS ctyp_kw, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dfis::varchar AS dfis, 
                        src:dfis_ref_compnr::integer AS dfis_ref_compnr, 
                        src:disc::numeric(38, 17) AS disc, 
                        src:efdt::datetime AS efdt, 
                        src:enip::integer AS enip, 
                        src:enip_kw::varchar AS enip_kw, 
                        src:esam::numeric(38, 17) AS esam, 
                        src:esam_cntc::numeric(38, 17) AS esam_cntc, 
                        src:esam_dwhc::numeric(38, 17) AS esam_dwhc, 
                        src:esam_homc::numeric(38, 17) AS esam_homc, 
                        src:esam_prjc::numeric(38, 17) AS esam_prjc, 
                        src:esam_refc::numeric(38, 17) AS esam_refc, 
                        src:esam_rpc1::numeric(38, 17) AS esam_rpc1, 
                        src:esam_rpc2::numeric(38, 17) AS esam_rpc2, 
                        src:esin::integer AS esin, 
                        src:esin_kw::varchar AS esin_kw, 
                        src:exdt::datetime AS exdt, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:exrs::varchar AS exrs, 
                        src:exrs_ref_compnr::integer AS exrs_ref_compnr, 
                        src:fcrt::integer AS fcrt, 
                        src:fcrt_kw::varchar AS fcrt_kw, 
                        src:fdis::integer AS fdis, 
                        src:fdis_kw::varchar AS fdis_kw, 
                        src:flmt::numeric(38, 17) AS flmt, 
                        src:flmt_cntc::numeric(38, 17) AS flmt_cntc, 
                        src:flmt_dwhc::numeric(38, 17) AS flmt_dwhc, 
                        src:flmt_homc::numeric(38, 17) AS flmt_homc, 
                        src:flmt_prjc::numeric(38, 17) AS flmt_prjc, 
                        src:flmt_refc::numeric(38, 17) AS flmt_refc, 
                        src:flmt_rpc1::numeric(38, 17) AS flmt_rpc1, 
                        src:flmt_rpc2::numeric(38, 17) AS flmt_rpc2, 
                        src:frvt::numeric(38, 17) AS frvt, 
                        src:hbpc::numeric(38, 17) AS hbpc, 
                        src:hldb::integer AS hldb, 
                        src:hldb_kw::varchar AS hldb_kw, 
                        src:hrsn::varchar AS hrsn, 
                        src:hrsn_ref_compnr::integer AS hrsn_ref_compnr, 
                        src:invm::integer AS invm, 
                        src:invm_kw::varchar AS invm_kw, 
                        src:itad::varchar AS itad, 
                        src:itad_ref_compnr::integer AS itad_ref_compnr, 
                        src:itbp::varchar AS itbp, 
                        src:itbp_ref_compnr::integer AS itbp_ref_compnr, 
                        src:itcn::varchar AS itcn, 
                        src:itcn_ref_compnr::integer AS itcn_ref_compnr, 
                        src:itod::numeric(38, 17) AS itod, 
                        src:itod_cntc::numeric(38, 17) AS itod_cntc, 
                        src:itod_dwhc::numeric(38, 17) AS itod_dwhc, 
                        src:itod_homc::numeric(38, 17) AS itod_homc, 
                        src:itod_prjc::numeric(38, 17) AS itod_prjc, 
                        src:itod_refc::numeric(38, 17) AS itod_refc, 
                        src:itod_rpc1::numeric(38, 17) AS itod_rpc1, 
                        src:itod_rpc2::numeric(38, 17) AS itod_rpc2, 
                        src:ivam::numeric(38, 17) AS ivam, 
                        src:lcrs::varchar AS lcrs, 
                        src:lcrs_ref_compnr::integer AS lcrs_ref_compnr, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:lpad::numeric(38, 17) AS lpad, 
                        src:lpin::numeric(38, 17) AS lpin, 
                        src:ltti::date AS ltti, 
                        src:mfad::varchar AS mfad, 
                        src:mfad_ref_compnr::integer AS mfad_ref_compnr, 
                        src:mfea::object AS mfea, 
                        src:mftp::integer AS mftp, 
                        src:mftp_kw::varchar AS mftp_kw, 
                        src:mftx::integer AS mftx, 
                        src:mftx_ref_compnr::integer AS mftx_ref_compnr, 
                        src:next_bldt::varchar AS next_bldt, 
                        src:ofad::varchar AS ofad, 
                        src:ofad_ref_compnr::integer AS ofad_ref_compnr, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:ofcn::varchar AS ofcn, 
                        src:ofcn_ref_compnr::integer AS ofcn_ref_compnr, 
                        src:osrp::varchar AS osrp, 
                        src:osrp_ref_compnr::integer AS osrp_ref_compnr, 
                        src:paym::varchar AS paym, 
                        src:pcba::numeric(38, 17) AS pcba, 
                        src:pceq::numeric(38, 17) AS pceq, 
                        src:pcic::numeric(38, 17) AS pcic, 
                        src:pcit::numeric(38, 17) AS pcit, 
                        src:pcnt::varchar AS pcnt, 
                        src:pcnt_ref_compnr::integer AS pcnt_ref_compnr, 
                        src:pcrf::object AS pcrf, 
                        src:pcsb::numeric(38, 17) AS pcsb, 
                        src:pcts::numeric(38, 17) AS pcts, 
                        src:pcwa::numeric(38, 17) AS pcwa, 
                        src:pfad::varchar AS pfad, 
                        src:pfad_ref_compnr::integer AS pfad_ref_compnr, 
                        src:pfbp::varchar AS pfbp, 
                        src:pfbp_ref_compnr::integer AS pfbp_ref_compnr, 
                        src:pfcn::varchar AS pfcn, 
                        src:pfcn_ref_compnr::integer AS pfcn_ref_compnr, 
                        src:pfob::integer AS pfob, 
                        src:pfob_kw::varchar AS pfob_kw, 
                        src:pgpm::integer AS pgpm, 
                        src:pgpm_kw::varchar AS pgpm_kw, 
                        src:plpc::numeric(38, 17) AS plpc, 
                        src:poin::integer AS poin, 
                        src:ppim::varchar AS ppim, 
                        src:ppim_ref_compnr::integer AS ppim_ref_compnr, 
                        src:pppc::numeric(38, 17) AS pppc, 
                        src:prrt::varchar AS prrt, 
                        src:prrt_ref_compnr::integer AS prrt_ref_compnr, 
                        src:prst::integer AS prst, 
                        src:prst_kw::varchar AS prst_kw, 
                        src:prtx::integer AS prtx, 
                        src:prtx_kw::varchar AS prtx_kw, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:ptpp::varchar AS ptpp, 
                        src:ptpp_ref_compnr::integer AS ptpp_ref_compnr, 
                        src:ptsh::numeric(38, 17) AS ptsh, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rdat::datetime AS rdat, 
                        src:rddt::datetime AS rddt, 
                        src:refb::object AS refb, 
                        src:refe::object AS refe, 
                        src:rfcu::object AS rfcu, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sadr::varchar AS sadr, 
                        src:sadr_ref_compnr::integer AS sadr_ref_compnr, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:speq::integer AS speq, 
                        src:speq_kw::varchar AS speq_kw, 
                        src:spic::integer AS spic, 
                        src:spic_kw::varchar AS spic_kw, 
                        src:spit::integer AS spit, 
                        src:spit_kw::varchar AS spit_kw, 
                        src:spsb::integer AS spsb, 
                        src:spsb_kw::varchar AS spsb_kw, 
                        src:spts::integer AS spts, 
                        src:spts_kw::varchar AS spts_kw, 
                        src:srvo::varchar AS srvo, 
                        src:srvo_ref_compnr::integer AS srvo_ref_compnr, 
                        src:stad::varchar AS stad, 
                        src:stad_ref_compnr::integer AS stad_ref_compnr, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:stcn::varchar AS stcn, 
                        src:stcn_ref_compnr::integer AS stcn_ref_compnr, 
                        src:styp::varchar AS styp, 
                        src:styp_ref_compnr::integer AS styp_ref_compnr, 
                        src:tcst::integer AS tcst, 
                        src:tcst_kw::varchar AS tcst_kw, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tins::integer AS tins, 
                        src:tins_kw::varchar AS tins_kw, 
                        src:trpr::numeric(38, 17) AS trpr, 
                        src:ubnk::integer AS ubnk, 
                        src:ubnk_kw::varchar AS ubnk_kw, 
                        src:ulpd::integer AS ulpd, 
                        src:ulpd_kw::varchar AS ulpd_kw, 
                        src:username::varchar AS username, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:cono,
                src:cnln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPCTM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acpn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:advn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bcrp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgrb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blck), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ceil), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clam_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnln_fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:codt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_bver_bnum_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfis_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:efdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:enip), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esam_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esin), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:exdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flmt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frvt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hldb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hrsn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpin), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltti), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mfad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mftp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mftx), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mftx_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcba), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pceq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcnt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcsb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcwa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfob), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pgpm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:poin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppim_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pppc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prrt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prtx), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptsh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:speq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srvo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tins), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubnk), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ulpd), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null