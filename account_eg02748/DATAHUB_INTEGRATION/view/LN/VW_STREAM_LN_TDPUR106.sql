CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR106 AS SELECT
                        src:adin::varchar AS adin, 
                        src:amta::numeric(38, 17) AS amta, 
                        src:ares::integer AS ares, 
                        src:ares_kw::varchar AS ares_kw, 
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:ccon::varchar AS ccon, 
                        src:ccon_ref_compnr::integer AS ccon_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_cvat_ref_compnr::integer AS ccty_cvat_ref_compnr, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:cdis_1::varchar AS cdis_1, 
                        src:cdis_10::varchar AS cdis_10, 
                        src:cdis_11::varchar AS cdis_11, 
                        src:cdis_2::varchar AS cdis_2, 
                        src:cdis_3::varchar AS cdis_3, 
                        src:cdis_4::varchar AS cdis_4, 
                        src:cdis_5::varchar AS cdis_5, 
                        src:cdis_6::varchar AS cdis_6, 
                        src:cdis_7::varchar AS cdis_7, 
                        src:cdis_8::varchar AS cdis_8, 
                        src:cdis_9::varchar AS cdis_9, 
                        src:ceno::varchar AS ceno, 
                        src:cfrw::varchar AS cfrw, 
                        src:cfrw_ref_compnr::integer AS cfrw_ref_compnr, 
                        src:citg::varchar AS citg, 
                        src:citg_ref_compnr::integer AS citg_ref_compnr, 
                        src:citt::varchar AS citt, 
                        src:citt_ref_compnr::integer AS citt_ref_compnr, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:cnat::integer AS cnat, 
                        src:cnat_kw::varchar AS cnat_kw, 
                        src:cncd::varchar AS cncd, 
                        src:cnty::integer AS cnty, 
                        src:cnty_kw::varchar AS cnty_kw, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:copy::integer AS copy, 
                        src:copy_kw::varchar AS copy_kw, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crbn::integer AS crbn, 
                        src:crbn_kw::varchar AS crbn_kw, 
                        src:crcd::varchar AS crcd, 
                        src:crcd_ref_compnr::integer AS crcd_ref_compnr, 
                        src:ctcd::varchar AS ctcd, 
                        src:ctcd_ref_compnr::integer AS ctcd_ref_compnr, 
                        src:cupp::varchar AS cupp, 
                        src:cupp_ref_compnr::integer AS cupp_ref_compnr, 
                        src:cuqp::varchar AS cuqp, 
                        src:cuqp_ref_compnr::integer AS cuqp_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvpp::numeric(38, 17) AS cvpp, 
                        src:cvqp::numeric(38, 17) AS cvqp, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:ddat::datetime AS ddat, 
                        src:deleted::boolean AS deleted, 
                        src:disc_1::numeric(38, 17) AS disc_1, 
                        src:disc_10::numeric(38, 17) AS disc_10, 
                        src:disc_11::numeric(38, 17) AS disc_11, 
                        src:disc_2::numeric(38, 17) AS disc_2, 
                        src:disc_3::numeric(38, 17) AS disc_3, 
                        src:disc_4::numeric(38, 17) AS disc_4, 
                        src:disc_5::numeric(38, 17) AS disc_5, 
                        src:disc_6::numeric(38, 17) AS disc_6, 
                        src:disc_7::numeric(38, 17) AS disc_7, 
                        src:disc_8::numeric(38, 17) AS disc_8, 
                        src:disc_9::numeric(38, 17) AS disc_9, 
                        src:dmde_1::varchar AS dmde_1, 
                        src:dmde_10::varchar AS dmde_10, 
                        src:dmde_11::varchar AS dmde_11, 
                        src:dmde_2::varchar AS dmde_2, 
                        src:dmde_3::varchar AS dmde_3, 
                        src:dmde_4::varchar AS dmde_4, 
                        src:dmde_5::varchar AS dmde_5, 
                        src:dmde_6::varchar AS dmde_6, 
                        src:dmde_7::varchar AS dmde_7, 
                        src:dmde_8::varchar AS dmde_8, 
                        src:dmde_9::varchar AS dmde_9, 
                        src:dmse_1::integer AS dmse_1, 
                        src:dmse_10::integer AS dmse_10, 
                        src:dmse_11::integer AS dmse_11, 
                        src:dmse_2::integer AS dmse_2, 
                        src:dmse_3::integer AS dmse_3, 
                        src:dmse_4::integer AS dmse_4, 
                        src:dmse_5::integer AS dmse_5, 
                        src:dmse_6::integer AS dmse_6, 
                        src:dmse_7::integer AS dmse_7, 
                        src:dmse_8::integer AS dmse_8, 
                        src:dmse_9::integer AS dmse_9, 
                        src:dmth_1::integer AS dmth_1, 
                        src:dmth_10::integer AS dmth_10, 
                        src:dmth_11::integer AS dmth_11, 
                        src:dmth_2::integer AS dmth_2, 
                        src:dmth_3::integer AS dmth_3, 
                        src:dmth_4::integer AS dmth_4, 
                        src:dmth_5::integer AS dmth_5, 
                        src:dmth_6::integer AS dmth_6, 
                        src:dmth_7::integer AS dmth_7, 
                        src:dmth_8::integer AS dmth_8, 
                        src:dmth_9::integer AS dmth_9, 
                        src:dmth_kw_1::varchar AS dmth_kw_1, 
                        src:dmth_kw_10::varchar AS dmth_kw_10, 
                        src:dmth_kw_11::varchar AS dmth_kw_11, 
                        src:dmth_kw_2::varchar AS dmth_kw_2, 
                        src:dmth_kw_3::varchar AS dmth_kw_3, 
                        src:dmth_kw_4::varchar AS dmth_kw_4, 
                        src:dmth_kw_5::varchar AS dmth_kw_5, 
                        src:dmth_kw_6::varchar AS dmth_kw_6, 
                        src:dmth_kw_7::varchar AS dmth_kw_7, 
                        src:dmth_kw_8::varchar AS dmth_kw_8, 
                        src:dmth_kw_9::varchar AS dmth_kw_9, 
                        src:dmty_1::integer AS dmty_1, 
                        src:dmty_10::integer AS dmty_10, 
                        src:dmty_11::integer AS dmty_11, 
                        src:dmty_2::integer AS dmty_2, 
                        src:dmty_3::integer AS dmty_3, 
                        src:dmty_4::integer AS dmty_4, 
                        src:dmty_5::integer AS dmty_5, 
                        src:dmty_6::integer AS dmty_6, 
                        src:dmty_7::integer AS dmty_7, 
                        src:dmty_8::integer AS dmty_8, 
                        src:dmty_9::integer AS dmty_9, 
                        src:dmty_kw_1::varchar AS dmty_kw_1, 
                        src:dmty_kw_10::varchar AS dmty_kw_10, 
                        src:dmty_kw_11::varchar AS dmty_kw_11, 
                        src:dmty_kw_2::varchar AS dmty_kw_2, 
                        src:dmty_kw_3::varchar AS dmty_kw_3, 
                        src:dmty_kw_4::varchar AS dmty_kw_4, 
                        src:dmty_kw_5::varchar AS dmty_kw_5, 
                        src:dmty_kw_6::varchar AS dmty_kw_6, 
                        src:dmty_kw_7::varchar AS dmty_kw_7, 
                        src:dmty_kw_8::varchar AS dmty_kw_8, 
                        src:dmty_kw_9::varchar AS dmty_kw_9, 
                        src:dorg_1::integer AS dorg_1, 
                        src:dorg_10::integer AS dorg_10, 
                        src:dorg_11::integer AS dorg_11, 
                        src:dorg_2::integer AS dorg_2, 
                        src:dorg_3::integer AS dorg_3, 
                        src:dorg_4::integer AS dorg_4, 
                        src:dorg_5::integer AS dorg_5, 
                        src:dorg_6::integer AS dorg_6, 
                        src:dorg_7::integer AS dorg_7, 
                        src:dorg_8::integer AS dorg_8, 
                        src:dorg_9::integer AS dorg_9, 
                        src:dorg_kw_1::varchar AS dorg_kw_1, 
                        src:dorg_kw_10::varchar AS dorg_kw_10, 
                        src:dorg_kw_11::varchar AS dorg_kw_11, 
                        src:dorg_kw_2::varchar AS dorg_kw_2, 
                        src:dorg_kw_3::varchar AS dorg_kw_3, 
                        src:dorg_kw_4::varchar AS dorg_kw_4, 
                        src:dorg_kw_5::varchar AS dorg_kw_5, 
                        src:dorg_kw_6::varchar AS dorg_kw_6, 
                        src:dorg_kw_7::varchar AS dorg_kw_7, 
                        src:dorg_kw_8::varchar AS dorg_kw_8, 
                        src:dorg_kw_9::varchar AS dorg_kw_9, 
                        src:dsor::integer AS dsor, 
                        src:dsor_kw::varchar AS dsor_kw, 
                        src:dssc_1::varchar AS dssc_1, 
                        src:dssc_10::varchar AS dssc_10, 
                        src:dssc_11::varchar AS dssc_11, 
                        src:dssc_2::varchar AS dssc_2, 
                        src:dssc_3::varchar AS dssc_3, 
                        src:dssc_4::varchar AS dssc_4, 
                        src:dssc_5::varchar AS dssc_5, 
                        src:dssc_6::varchar AS dssc_6, 
                        src:dssc_7::varchar AS dssc_7, 
                        src:dssc_8::varchar AS dssc_8, 
                        src:dssc_9::varchar AS dssc_9, 
                        src:dtrm::integer AS dtrm, 
                        src:dtrm_kw::varchar AS dtrm_kw, 
                        src:edat::datetime AS edat, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:elgb::integer AS elgb, 
                        src:elgb_kw::varchar AS elgb_kw, 
                        src:eolf::integer AS eolf, 
                        src:eolf_kw::varchar AS eolf_kw, 
                        src:etpc::integer AS etpc, 
                        src:etpc_kw::varchar AS etpc_kw, 
                        src:exdt::datetime AS exdt, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:gefo::integer AS gefo, 
                        src:gefo_kw::varchar AS gefo_kw, 
                        src:ipla::integer AS ipla, 
                        src:ipla_kw::varchar AS ipla_kw, 
                        src:issp::integer AS issp, 
                        src:issp_kw::varchar AS issp_kw, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:kofl::integer AS kofl, 
                        src:kofl_kw::varchar AS kofl_kw, 
                        src:lbdt::datetime AS lbdt, 
                        src:lbuy::integer AS lbuy, 
                        src:lbuy_kw::varchar AS lbuy_kw, 
                        src:lccl::varchar AS lccl, 
                        src:lccl_ref_compnr::integer AS lccl_ref_compnr, 
                        src:lcmp::integer AS lcmp, 
                        src:lcmp_ref_compnr::integer AS lcmp_ref_compnr, 
                        src:ldam_1::numeric(38, 17) AS ldam_1, 
                        src:ldam_10::numeric(38, 17) AS ldam_10, 
                        src:ldam_11::numeric(38, 17) AS ldam_11, 
                        src:ldam_2::numeric(38, 17) AS ldam_2, 
                        src:ldam_3::numeric(38, 17) AS ldam_3, 
                        src:ldam_4::numeric(38, 17) AS ldam_4, 
                        src:ldam_5::numeric(38, 17) AS ldam_5, 
                        src:ldam_6::numeric(38, 17) AS ldam_6, 
                        src:ldam_7::numeric(38, 17) AS ldam_7, 
                        src:ldam_8::numeric(38, 17) AS ldam_8, 
                        src:ldam_9::numeric(38, 17) AS ldam_9, 
                        src:lead::integer AS lead, 
                        src:leng::numeric(38, 17) AS leng, 
                        src:leun::integer AS leun, 
                        src:leun_kw::varchar AS leun_kw, 
                        src:ltyp::integer AS ltyp, 
                        src:ltyp_kw::varchar AS ltyp_kw, 
                        src:mfdt::datetime AS mfdt, 
                        src:mpnr::varchar AS mpnr, 
                        src:mpnr_cmnf_ref_compnr::integer AS mpnr_cmnf_ref_compnr, 
                        src:note::varchar AS note, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:pbor::integer AS pbor, 
                        src:pbor_kw::varchar AS pbor_kw, 
                        src:pegd::integer AS pegd, 
                        src:pegd_kw::varchar AS pegd_kw, 
                        src:plin::integer AS plin, 
                        src:pmde::varchar AS pmde, 
                        src:pmse::integer AS pmse, 
                        src:pono::integer AS pono, 
                        src:porg::integer AS porg, 
                        src:porg_kw::varchar AS porg_kw, 
                        src:prbk::varchar AS prbk, 
                        src:prbk_ref_compnr::integer AS prbk_ref_compnr, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:prih_1::numeric(38, 17) AS prih_1, 
                        src:prih_2::numeric(38, 17) AS prih_2, 
                        src:prih_3::numeric(38, 17) AS prih_3, 
                        src:prsg::varchar AS prsg, 
                        src:prsg_ref_compnr::integer AS prsg_ref_compnr, 
                        src:pseq::integer AS pseq, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:qdat::datetime AS qdat, 
                        src:qimf::numeric(38, 17) AS qimf, 
                        src:qimi::numeric(38, 17) AS qimi, 
                        src:qono::varchar AS qono, 
                        src:qono_otbp_ref_compnr::integer AS qono_otbp_ref_compnr, 
                        src:qono_ref_compnr::integer AS qono_ref_compnr, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:qspa::integer AS qspa, 
                        src:qspa_kw::varchar AS qspa_kw, 
                        src:rank::integer AS rank, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rcsi::varchar AS rcsi, 
                        src:rcsi_ref_compnr::integer AS rcsi_ref_compnr, 
                        src:revi::varchar AS revi, 
                        src:rtdt::datetime AS rtdt, 
                        src:sdat::datetime AS sdat, 
                        src:sdsc::integer AS sdsc, 
                        src:sdsc_kw::varchar AS sdsc_kw, 
                        src:send::integer AS send, 
                        src:send_kw::varchar AS send_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serv::varchar AS serv, 
                        src:serv_ref_compnr::integer AS serv_ref_compnr, 
                        src:sfad::varchar AS sfad, 
                        src:sfad_ref_compnr::integer AS sfad_ref_compnr, 
                        src:sfbp::varchar AS sfbp, 
                        src:sfbp_ref_compnr::integer AS sfbp_ref_compnr, 
                        src:sfcn::varchar AS sfcn, 
                        src:sfcn_ref_compnr::integer AS sfcn_ref_compnr, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:srnb::integer AS srnb, 
                        src:stdc::numeric(38, 17) AS stdc, 
                        src:suti::numeric(38, 17) AS suti, 
                        src:sutu::integer AS sutu, 
                        src:sutu_kw::varchar AS sutu_kw, 
                        src:tapr::numeric(38, 17) AS tapr, 
                        src:thic::numeric(38, 17) AS thic, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpbk::varchar AS tpbk, 
                        src:tpbk_ref_compnr::integer AS tpbk_ref_compnr, 
                        src:tpbk_tpbl_ref_compnr::integer AS tpbk_tpbl_ref_compnr, 
                        src:tpbl::integer AS tpbl, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:txtb::integer AS txtb, 
                        src:txtb_ref_compnr::integer AS txtb_ref_compnr, 
                        src:username::varchar AS username, 
                        src:warr::integer AS warr, 
                        src:warr_kw::varchar AS warr_kw, 
                        src:widt::numeric(38, 17) AS widt, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:pono,
                src:qono,
                src:srnb,
                src:otbp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR106 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ares), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copy), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crbn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuqp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvqp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dtrm), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:edat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elgb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eolf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:etpc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:exdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gefo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipla), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:issp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kofl), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lbdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lbuy), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lead), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:leng), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:leun), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltyp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:mfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pbor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pegd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmse), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:porg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prbk_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prih_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prih_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prih_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:qdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qimf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qimi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qono_otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qspa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rank), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcsi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rtdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sdsc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:send), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srnb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stdc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:suti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sutu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tapr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:thic), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpbk_tpbl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:warr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:widt), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null