CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM700 AS SELECT
                        src:acpn::integer AS acpn, 
                        src:acpn_kw::varchar AS acpn_kw, 
                        src:adin::varchar AS adin, 
                        src:amts::numeric(38, 17) AS amts, 
                        src:amts_cntc::numeric(38, 17) AS amts_cntc, 
                        src:amts_dwhc::numeric(38, 17) AS amts_dwhc, 
                        src:amts_homc::numeric(38, 17) AS amts_homc, 
                        src:amts_prjc::numeric(38, 17) AS amts_prjc, 
                        src:amts_refc::numeric(38, 17) AS amts_refc, 
                        src:amts_rpc1::numeric(38, 17) AS amts_rpc1, 
                        src:amts_rpc2::numeric(38, 17) AS amts_rpc2, 
                        src:aqan::numeric(38, 17) AS aqan, 
                        src:baor::integer AS baor, 
                        src:baor_kw::varchar AS baor_kw, 
                        src:bgrb::integer AS bgrb, 
                        src:bgrb_kw::varchar AS bgrb_kw, 
                        src:bgrq::integer AS bgrq, 
                        src:bgrq_kw::varchar AS bgrq_kw, 
                        src:blck::integer AS blck, 
                        src:blck_kw::varchar AS blck_kw, 
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:bqan::numeric(38, 17) AS bqan, 
                        src:buni::varchar AS buni, 
                        src:buni_ref_compnr::integer AS buni_ref_compnr, 
                        src:cact::varchar AS cact, 
                        src:camt::numeric(38, 17) AS camt, 
                        src:camt_cntc::numeric(38, 17) AS camt_cntc, 
                        src:camt_dwhc::numeric(38, 17) AS camt_dwhc, 
                        src:camt_homc::numeric(38, 17) AS camt_homc, 
                        src:camt_prjc::numeric(38, 17) AS camt_prjc, 
                        src:camt_refc::numeric(38, 17) AS camt_refc, 
                        src:camt_rpc1::numeric(38, 17) AS camt_rpc1, 
                        src:camt_rpc2::numeric(38, 17) AS camt_rpc2, 
                        src:carr::varchar AS carr, 
                        src:carr_ref_compnr::integer AS carr_ref_compnr, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccfm::integer AS ccfm, 
                        src:ccfm_kw::varchar AS ccfm_kw, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cddt::datetime AS cddt, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:ceno::varchar AS ceno, 
                        src:citt::varchar AS citt, 
                        src:citt_ref_compnr::integer AS citt_ref_compnr, 
                        src:clot::varchar AS clot, 
                        src:cnac::varchar AS cnac, 
                        src:cncd::varchar AS cncd, 
                        src:cnel::varchar AS cnel, 
                        src:cnln::varchar AS cnln, 
                        src:cnpl::varchar AS cnpl, 
                        src:cnpr::varchar AS cnpr, 
                        src:cnpr_cnel_ref_compnr::integer AS cnpr_cnel_ref_compnr, 
                        src:cnpr_cnpl_cnac_ref_compnr::integer AS cnpr_cnpl_cnac_ref_compnr, 
                        src:cnpr_ref_compnr::integer AS cnpr_ref_compnr, 
                        src:cocu::varchar AS cocu, 
                        src:cocu_ref_compnr::integer AS cocu_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cpla_milt_ref_compnr::integer AS cprj_cpla_milt_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cpva::integer AS cpva, 
                        src:cqan::numeric(38, 17) AS cqan, 
                        src:crbn::integer AS crbn, 
                        src:crbn_kw::varchar AS crbn_kw, 
                        src:crby::varchar AS crby, 
                        src:crdt::datetime AS crdt, 
                        src:crte::varchar AS crte, 
                        src:crte_ref_compnr::integer AS crte_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cspg::integer AS cspg, 
                        src:cspg_kw::varchar AS cspg_kw, 
                        src:cstl::varchar AS cstl, 
                        src:cstv::numeric(38, 17) AS cstv, 
                        src:cstv_cntc::numeric(38, 17) AS cstv_cntc, 
                        src:cstv_dwhc::numeric(38, 17) AS cstv_dwhc, 
                        src:cstv_homc::numeric(38, 17) AS cstv_homc, 
                        src:cstv_prjc::numeric(38, 17) AS cstv_prjc, 
                        src:cstv_refc::numeric(38, 17) AS cstv_refc, 
                        src:cstv_rpc1::numeric(38, 17) AS cstv_rpc1, 
                        src:cstv_rpc2::numeric(38, 17) AS cstv_rpc2, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:delt::integer AS delt, 
                        src:delt_kw::varchar AS delt_kw, 
                        src:disc::numeric(38, 17) AS disc, 
                        src:dlor::integer AS dlor, 
                        src:dlor_kw::varchar AS dlor_kw, 
                        src:dlpt::varchar AS dlpt, 
                        src:dlvr::integer AS dlvr, 
                        src:dorg::integer AS dorg, 
                        src:dorg_kw::varchar AS dorg_kw, 
                        src:dspr::integer AS dspr, 
                        src:dspr_kw::varchar AS dspr_kw, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:exrs::varchar AS exrs, 
                        src:exrs_ref_compnr::integer AS exrs_ref_compnr, 
                        src:iddt::datetime AS iddt, 
                        src:inst::varchar AS inst, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:item_site_ref_compnr::integer AS item_site_ref_compnr, 
                        src:lcrq::integer AS lcrq, 
                        src:lcrq_kw::varchar AS lcrq_kw, 
                        src:ldam::numeric(38, 17) AS ldam, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:loco::varchar AS loco, 
                        src:lssn::varchar AS lssn, 
                        src:lssn_ref_compnr::integer AS lssn_ref_compnr, 
                        src:ltsl::integer AS ltsl, 
                        src:ltsl_kw::varchar AS ltsl_kw, 
                        src:milt::varchar AS milt, 
                        src:mrlc::varchar AS mrlc, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:oqan::numeric(38, 17) AS oqan, 
                        src:orno::varchar AS orno, 
                        src:osch::integer AS osch, 
                        src:ouni::varchar AS ouni, 
                        src:ouni_ref_compnr::integer AS ouni_ref_compnr, 
                        src:pass::integer AS pass, 
                        src:pass_kw::varchar AS pass_kw, 
                        src:paym::varchar AS paym, 
                        src:pddt::datetime AS pddt, 
                        src:pono::integer AS pono, 
                        src:porg::integer AS porg, 
                        src:porg_kw::varchar AS porg_kw, 
                        src:prdt::datetime AS prdt, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:prsg::varchar AS prsg, 
                        src:prsg_ref_compnr::integer AS prsg_ref_compnr, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:rddt::datetime AS rddt, 
                        src:rfcu::object AS rfcu, 
                        src:rfdl::integer AS rfdl, 
                        src:rfic::integer AS rfic, 
                        src:rfid::integer AS rfid, 
                        src:rfit::varchar AS rfit, 
                        src:rfsc::integer AS rfsc, 
                        src:rfsh::varchar AS rfsh, 
                        src:rlcu::object AS rlcu, 
                        src:rown::integer AS rown, 
                        src:rown_kw::varchar AS rown_kw, 
                        src:rqan::numeric(38, 17) AS rqan, 
                        src:rrsn::varchar AS rrsn, 
                        src:rrsn_ref_compnr::integer AS rrsn_ref_compnr, 
                        src:rtcc_1::numeric(38, 17) AS rtcc_1, 
                        src:rtcc_2::numeric(38, 17) AS rtcc_2, 
                        src:rtcc_3::numeric(38, 17) AS rtcc_3, 
                        src:rtcs_1::numeric(38, 17) AS rtcs_1, 
                        src:rtcs_2::numeric(38, 17) AS rtcs_2, 
                        src:rtcs_3::numeric(38, 17) AS rtcs_3, 
                        src:rtfc_1::integer AS rtfc_1, 
                        src:rtfc_2::integer AS rtfc_2, 
                        src:rtfc_3::integer AS rtfc_3, 
                        src:rtfs_1::integer AS rtfs_1, 
                        src:rtfs_2::integer AS rtfs_2, 
                        src:rtfs_3::integer AS rtfs_3, 
                        src:rtor::integer AS rtor, 
                        src:rtor_kw::varchar AS rtor_kw, 
                        src:sacu::varchar AS sacu, 
                        src:sacu_ref_compnr::integer AS sacu_ref_compnr, 
                        src:samt::numeric(38, 17) AS samt, 
                        src:samt_cntc::numeric(38, 17) AS samt_cntc, 
                        src:samt_dwhc::numeric(38, 17) AS samt_dwhc, 
                        src:samt_homc::numeric(38, 17) AS samt_homc, 
                        src:samt_prjc::numeric(38, 17) AS samt_prjc, 
                        src:samt_refc::numeric(38, 17) AS samt_refc, 
                        src:samt_rpc1::numeric(38, 17) AS samt_rpc1, 
                        src:samt_rpc2::numeric(38, 17) AS samt_rpc2, 
                        src:schd::integer AS schd, 
                        src:sdsc::integer AS sdsc, 
                        src:sdsc_kw::varchar AS sdsc_kw, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serl::varchar AS serl, 
                        src:sern::integer AS sern, 
                        src:serv::varchar AS serv, 
                        src:serv_ref_compnr::integer AS serv_ref_compnr, 
                        src:shcn::integer AS shcn, 
                        src:shcn_kw::varchar AS shcn_kw, 
                        src:sign::integer AS sign, 
                        src:sign_kw::varchar AS sign_kw, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:sqan::numeric(38, 17) AS sqan, 
                        src:srsl::integer AS srsl, 
                        src:srsl_kw::varchar AS srsl_kw, 
                        src:stad::varchar AS stad, 
                        src:stad_dlpt_ref_compnr::integer AS stad_dlpt_ref_compnr, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:stcn::varchar AS stcn, 
                        src:stcn_ref_compnr::integer AS stcn_ref_compnr, 
                        src:suni::varchar AS suni, 
                        src:suni_ref_compnr::integer AS suni_ref_compnr, 
                        src:tact::varchar AS tact, 
                        src:tatd::varchar AS tatd, 
                        src:tatd_ref_compnr::integer AS tatd_ref_compnr, 
                        src:tcco::varchar AS tcco, 
                        src:tcco_ref_compnr::integer AS tcco_ref_compnr, 
                        src:tcff::integer AS tcff, 
                        src:tcff_kw::varchar AS tcff_kw, 
                        src:tcst::integer AS tcst, 
                        src:tcst_kw::varchar AS tcst_kw, 
                        src:tdel::integer AS tdel, 
                        src:tdel_kw::varchar AS tdel_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpla::varchar AS tpla, 
                        src:tprj::varchar AS tprj, 
                        src:tprj_ref_compnr::integer AS tprj_ref_compnr, 
                        src:tprj_tpla_ref_compnr::integer AS tprj_tpla_ref_compnr, 
                        src:tprj_tpla_tact_ref_compnr::integer AS tprj_tpla_tact_ref_compnr, 
                        src:tprj_tspa_ref_compnr::integer AS tprj_tspa_ref_compnr, 
                        src:tprj_tstl_ref_compnr::integer AS tprj_tstl_ref_compnr, 
                        src:tspa::varchar AS tspa, 
                        src:tstl::varchar AS tstl, 
                        src:twar::varchar AS twar, 
                        src:twar_ref_compnr::integer AS twar_ref_compnr, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:unbd::integer AS unbd, 
                        src:unbd_kw::varchar AS unbd_kw, 
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
                src:cnln,
                src:sern,
                src:cprj,
                src:dlvr,
                src:cono,
                src:cpla,
                src:schd,
                src:cspa,
                src:cact  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM700 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acpn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amts_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgrb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blck), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:buni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccfm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_cnel_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_cnpl_cnac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cocu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_milt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpva), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crbn), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:delt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dlor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dlvr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dspr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:iddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ouni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pass), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:porg), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfdl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfid), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rown), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrsn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtcs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtfs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:schd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sdsc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shcn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sign), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:suni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tatd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcff), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tdel), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprj_tpla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprj_tpla_tact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprj_tspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprj_tstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:twar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unbd), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null