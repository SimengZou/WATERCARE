CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC350 AS SELECT
                        src:afin::integer AS afin, 
                        src:afin_kw::varchar AS afin_kw, 
                        src:amhc_1::numeric(38, 17) AS amhc_1, 
                        src:amhc_2::numeric(38, 17) AS amhc_2, 
                        src:amhc_3::numeric(38, 17) AS amhc_3, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:apdt::datetime AS apdt, 
                        src:appr::varchar AS appr, 
                        src:bfpp::integer AS bfpp, 
                        src:bfpp_kw::varchar AS bfpp_kw, 
                        src:bfrf::integer AS bfrf, 
                        src:bfrf_kw::varchar AS bfrf_kw, 
                        src:bgam::numeric(38, 17) AS bgam, 
                        src:blnc::numeric(38, 17) AS blnc, 
                        src:blnc_cntc::numeric(38, 17) AS blnc_cntc, 
                        src:blnc_dwhc::numeric(38, 17) AS blnc_dwhc, 
                        src:blnc_homc::numeric(38, 17) AS blnc_homc, 
                        src:blnc_prjc::numeric(38, 17) AS blnc_prjc, 
                        src:blnc_refc::numeric(38, 17) AS blnc_refc, 
                        src:blnc_rpc1::numeric(38, 17) AS blnc_rpc1, 
                        src:blnc_rpc2::numeric(38, 17) AS blnc_rpc2, 
                        src:btcs::numeric(38, 17) AS btcs, 
                        src:cact::varchar AS cact, 
                        src:camc::numeric(38, 17) AS camc, 
                        src:camc_cntc::numeric(38, 17) AS camc_cntc, 
                        src:camc_dwhc::numeric(38, 17) AS camc_dwhc, 
                        src:camc_prjc::numeric(38, 17) AS camc_prjc, 
                        src:camc_refc::numeric(38, 17) AS camc_refc, 
                        src:camr::numeric(38, 17) AS camr, 
                        src:camr_cntc::numeric(38, 17) AS camr_cntc, 
                        src:camr_dwhc::numeric(38, 17) AS camr_dwhc, 
                        src:camr_prjc::numeric(38, 17) AS camr_prjc, 
                        src:camr_refc::numeric(38, 17) AS camr_refc, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdru::datetime AS cdru, 
                        src:cert::varchar AS cert, 
                        src:cert_ref_compnr::integer AS cert_ref_compnr, 
                        src:cfpo::integer AS cfpo, 
                        src:cfpo_kw::varchar AS cfpo_kw, 
                        src:chcc_1::numeric(38, 17) AS chcc_1, 
                        src:chcc_2::numeric(38, 17) AS chcc_2, 
                        src:chcc_3::numeric(38, 17) AS chcc_3, 
                        src:chcr_1::numeric(38, 17) AS chcr_1, 
                        src:chcr_2::numeric(38, 17) AS chcr_2, 
                        src:chcr_3::numeric(38, 17) AS chcr_3, 
                        src:clmg::numeric(38, 17) AS clmg, 
                        src:cmfb::integer AS cmfb, 
                        src:cmfb_kw::varchar AS cmfb_kw, 
                        src:cmpc::integer AS cmpc, 
                        src:cmpc_kw::varchar AS cmpc_kw, 
                        src:cnln::varchar AS cnln, 
                        src:cogs::integer AS cogs, 
                        src:cogs_kw::varchar AS cogs_kw, 
                        src:comf::integer AS comf, 
                        src:comf_kw::varchar AS comf_kw, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:copr::numeric(38, 17) AS copr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cprv::varchar AS cprv, 
                        src:cprv_ref_compnr::integer AS cprv_ref_compnr, 
                        src:cptc::varchar AS cptc, 
                        src:cptc_year_peri_ref_compnr::integer AS cptc_year_peri_ref_compnr, 
                        src:cptf::varchar AS cptf, 
                        src:cptf_fyea_fper_ref_compnr::integer AS cptf_fyea_fper_ref_compnr, 
                        src:crdc::datetime AS crdc, 
                        src:crdr::datetime AS crdr, 
                        src:crfc_1::integer AS crfc_1, 
                        src:crfc_2::integer AS crfc_2, 
                        src:crfc_3::integer AS crfc_3, 
                        src:crfr_1::integer AS crfr_1, 
                        src:crfr_2::integer AS crfr_2, 
                        src:crfr_3::integer AS crfr_3, 
                        src:crtc_1::numeric(38, 17) AS crtc_1, 
                        src:crtc_2::numeric(38, 17) AS crtc_2, 
                        src:crtc_3::numeric(38, 17) AS crtc_3, 
                        src:crtr_1::numeric(38, 17) AS crtr_1, 
                        src:crtr_2::numeric(38, 17) AS crtr_2, 
                        src:crtr_3::numeric(38, 17) AS crtr_3, 
                        src:cser::integer AS cser, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:ctic::numeric(38, 17) AS ctic, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:drun::datetime AS drun, 
                        src:earf::numeric(38, 17) AS earf, 
                        src:estc::numeric(38, 17) AS estc, 
                        src:etcm::numeric(38, 17) AS etcm, 
                        src:exrv::numeric(38, 17) AS exrv, 
                        src:farv::numeric(38, 17) AS farv, 
                        src:fper::integer AS fper, 
                        src:frgd::datetime AS frgd, 
                        src:fyea::integer AS fyea, 
                        src:guid::varchar AS guid, 
                        src:heac::numeric(38, 17) AS heac, 
                        src:hetc::numeric(38, 17) AS hetc, 
                        src:hrbg::numeric(38, 17) AS hrbg, 
                        src:hrct::numeric(38, 17) AS hrct, 
                        src:irby::integer AS irby, 
                        src:irby_kw::varchar AS irby_kw, 
                        src:iror::integer AS iror, 
                        src:iror_kw::varchar AS iror_kw, 
                        src:irsc::integer AS irsc, 
                        src:irsc_kw::varchar AS irsc_kw, 
                        src:lmus::varchar AS lmus, 
                        src:ltdt::datetime AS ltdt, 
                        src:pcmp::numeric(38, 17) AS pcmp, 
                        src:peri::integer AS peri, 
                        src:pfob::integer AS pfob, 
                        src:pfob_kw::varchar AS pfob_kw, 
                        src:post::integer AS post, 
                        src:post_kw::varchar AS post_kw, 
                        src:prpc::numeric(38, 17) AS prpc, 
                        src:prvt::integer AS prvt, 
                        src:prvt_kw::varchar AS prvt_kw, 
                        src:pstb::integer AS pstb, 
                        src:pstb_kw::varchar AS pstb_kw, 
                        src:ramc::numeric(38, 17) AS ramc, 
                        src:ramc_cntc::numeric(38, 17) AS ramc_cntc, 
                        src:ramc_dwhc::numeric(38, 17) AS ramc_dwhc, 
                        src:ramc_prjc::numeric(38, 17) AS ramc_prjc, 
                        src:ramc_refc::numeric(38, 17) AS ramc_refc, 
                        src:ramr::numeric(38, 17) AS ramr, 
                        src:ramr_cntc::numeric(38, 17) AS ramr_cntc, 
                        src:ramr_dwhc::numeric(38, 17) AS ramr_dwhc, 
                        src:ramr_prjc::numeric(38, 17) AS ramr_prjc, 
                        src:ramr_refc::numeric(38, 17) AS ramr_refc, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rdat::datetime AS rdat, 
                        src:rert::varchar AS rert, 
                        src:rert_ref_compnr::integer AS rert_ref_compnr, 
                        src:revl::numeric(38, 17) AS revl, 
                        src:revr::integer AS revr, 
                        src:revr_kw::varchar AS revr_kw, 
                        src:revt::numeric(38, 17) AS revt, 
                        src:rgdt::datetime AS rgdt, 
                        src:rhcc_1::numeric(38, 17) AS rhcc_1, 
                        src:rhcc_2::numeric(38, 17) AS rhcc_2, 
                        src:rhcc_3::numeric(38, 17) AS rhcc_3, 
                        src:rhcr_1::numeric(38, 17) AS rhcr_1, 
                        src:rhcr_2::numeric(38, 17) AS rhcr_2, 
                        src:rhcr_3::numeric(38, 17) AS rhcr_3, 
                        src:rrby::integer AS rrby, 
                        src:rrby_kw::varchar AS rrby_kw, 
                        src:rrdc::datetime AS rrdc, 
                        src:rrdr::datetime AS rrdr, 
                        src:rrfc_1::integer AS rrfc_1, 
                        src:rrfc_2::integer AS rrfc_2, 
                        src:rrfc_3::integer AS rrfc_3, 
                        src:rrfr_1::integer AS rrfr_1, 
                        src:rrfr_2::integer AS rrfr_2, 
                        src:rrfr_3::integer AS rrfr_3, 
                        src:rrtc_1::numeric(38, 17) AS rrtc_1, 
                        src:rrtc_2::numeric(38, 17) AS rrtc_2, 
                        src:rrtc_3::numeric(38, 17) AS rrtc_3, 
                        src:rrtr_1::numeric(38, 17) AS rrtr_1, 
                        src:rrtr_2::numeric(38, 17) AS rrtr_2, 
                        src:rrtr_3::numeric(38, 17) AS rrtr_3, 
                        src:rtco::varchar AS rtco, 
                        src:rtco_ref_compnr::integer AS rtco_ref_compnr, 
                        src:rtre::varchar AS rtre, 
                        src:rtre_ref_compnr::integer AS rtre_ref_compnr, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:scds::object AS scds, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serb::integer AS serb, 
                        src:sern::integer AS sern, 
                        src:timestamp::datetime AS timestamp, 
                        src:trpr::numeric(38, 17) AS trpr, 
                        src:txtn::integer AS txtn, 
                        src:txtn_ref_compnr::integer AS txtn_ref_compnr, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:cspa,
                src:sern,
                src:cact,
                src:cprj,
                src:cstl  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC350 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:afin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amhc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amhc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amhc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:apdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfrf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blnc_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btcs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camc_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camc_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camr_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camr_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camr_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camr_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cdru), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cert_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chcc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chcc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chcc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chcr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chcr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chcr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clmg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmfb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cogs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprv_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptf_fyea_fper_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdc), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdr), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cser), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctic), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:drun), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:earf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:estc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:etcm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exrv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:farv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fper), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:frgd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fyea), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:heac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hetc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hrbg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hrct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:irby), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iror), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:irsc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:peri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfob), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:post), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prvt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pstb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramc_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramc_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramr_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramr_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramr_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ramr_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rert_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rgdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhcc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhcc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhcc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhcr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhcr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhcr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrby), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rrdc), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rrdr), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrfc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrfc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrfc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrfr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrfr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrfr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtre_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:serb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null