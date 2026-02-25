CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPCTM100 AS SELECT
                        src:acpn::integer AS acpn, 
                        src:acpn_kw::varchar AS acpn_kw, 
                        src:adin::varchar AS adin, 
                        src:adtp::integer AS adtp, 
                        src:adtp_kw::varchar AS adtp_kw, 
                        src:advn::integer AS advn, 
                        src:advn_kw::varchar AS advn_kw, 
                        src:bcrp::integer AS bcrp, 
                        src:bcrp_kw::varchar AS bcrp_kw, 
                        src:blcl::varchar AS blcl, 
                        src:blcl_ref_compnr::integer AS blcl_ref_compnr, 
                        src:bnum::varchar AS bnum, 
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
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cfac::varchar AS cfac, 
                        src:cfac_ref_compnr::integer AS cfac_ref_compnr, 
                        src:cidm::varchar AS cidm, 
                        src:cidm_ref_compnr::integer AS cidm_ref_compnr, 
                        src:cinm::varchar AS cinm, 
                        src:cinm_ref_compnr::integer AS cinm_ref_compnr, 
                        src:cmng::varchar AS cmng, 
                        src:cmng_ref_compnr::integer AS cmng_ref_compnr, 
                        src:codt::datetime AS codt, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_fcmp::integer AS cono_fcmp, 
                        src:copr::numeric(38, 17) AS copr, 
                        src:copr_dwhc::numeric(38, 17) AS copr_dwhc, 
                        src:copr_homc::numeric(38, 17) AS copr_homc, 
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
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dfis::varchar AS dfis, 
                        src:dfis_ref_compnr::integer AS dfis_ref_compnr, 
                        src:disc::numeric(38, 17) AS disc, 
                        src:efdt::datetime AS efdt, 
                        src:enip::integer AS enip, 
                        src:enip_kw::varchar AS enip_kw, 
                        src:exdt::datetime AS exdt, 
                        src:fcrt::integer AS fcrt, 
                        src:fcrt_kw::varchar AS fcrt_kw, 
                        src:fdis::integer AS fdis, 
                        src:fdis_kw::varchar AS fdis_kw, 
                        src:frvt::numeric(38, 17) AS frvt, 
                        src:guid::varchar AS guid, 
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
                        src:itod_dwhc::numeric(38, 17) AS itod_dwhc, 
                        src:itod_homc::numeric(38, 17) AS itod_homc, 
                        src:itod_refc::numeric(38, 17) AS itod_refc, 
                        src:itod_rpc1::numeric(38, 17) AS itod_rpc1, 
                        src:itod_rpc2::numeric(38, 17) AS itod_rpc2, 
                        src:ivam::numeric(38, 17) AS ivam, 
                        src:ivam_dwhc::numeric(38, 17) AS ivam_dwhc, 
                        src:ivam_homc::numeric(38, 17) AS ivam_homc, 
                        src:ivam_refc::numeric(38, 17) AS ivam_refc, 
                        src:ivam_rpc1::numeric(38, 17) AS ivam_rpc1, 
                        src:ivam_rpc2::numeric(38, 17) AS ivam_rpc2, 
                        src:lcrs::varchar AS lcrs, 
                        src:lcrs_ref_compnr::integer AS lcrs_ref_compnr, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:lpad::numeric(38, 17) AS lpad, 
                        src:lpin::numeric(38, 17) AS lpin, 
                        src:ltti::date AS ltti, 
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
                        src:pcwa::numeric(38, 17) AS pcwa, 
                        src:pfad::varchar AS pfad, 
                        src:pfad_ref_compnr::integer AS pfad_ref_compnr, 
                        src:pfbp::varchar AS pfbp, 
                        src:pfbp_ref_compnr::integer AS pfbp_ref_compnr, 
                        src:pfcn::varchar AS pfcn, 
                        src:pfcn_ref_compnr::integer AS pfcn_ref_compnr, 
                        src:pgpm::integer AS pgpm, 
                        src:pgpm_kw::varchar AS pgpm_kw, 
                        src:plpc::numeric(38, 17) AS plpc, 
                        src:pmng::varchar AS pmng, 
                        src:pmng_ref_compnr::integer AS pmng_ref_compnr, 
                        src:poin::integer AS poin, 
                        src:ppim::varchar AS ppim, 
                        src:ppim_ref_compnr::integer AS ppim_ref_compnr, 
                        src:pppc::numeric(38, 17) AS pppc, 
                        src:prgm::varchar AS prgm, 
                        src:prgm_ref_compnr::integer AS prgm_ref_compnr, 
                        src:prst::integer AS prst, 
                        src:prst_kw::varchar AS prst_kw, 
                        src:prtx::integer AS prtx, 
                        src:prtx_kw::varchar AS prtx_kw, 
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
                        src:refb::object AS refb, 
                        src:refe::object AS refe, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:styp::varchar AS styp, 
                        src:styp_ref_compnr::integer AS styp_ref_compnr, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tins::integer AS tins, 
                        src:tins_kw::varchar AS tins_kw, 
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
                src:cono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPCTM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acpn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:advn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bcrp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccam_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccot_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cidm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cinm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:codt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_homc), '0'), 38, 10) is not null and 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfis_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:efdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:enip), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:exdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frvt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hldb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hrsn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itod_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ivam_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lpin), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltti), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcba), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcwa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pgpm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:poin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppim_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pppc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prgm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prtx), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptsh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tins), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ulpd), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null