CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDSLS400 AS SELECT
                        src:acdt::datetime AS acdt, 
                        src:adin::varchar AS adin, 
                        src:agen::integer AS agen, 
                        src:agen_kw::varchar AS agen_kw, 
                        src:airp::integer AS airp, 
                        src:airp_kw::varchar AS airp_kw, 
                        src:akcd::varchar AS akcd, 
                        src:akcd_ref_compnr::integer AS akcd_ref_compnr, 
                        src:bgrb::integer AS bgrb, 
                        src:bgrb_kw::varchar AS bgrb_kw, 
                        src:bgrq::integer AS bgrq, 
                        src:bgrq_kw::varchar AS bgrq_kw, 
                        src:bkyn::integer AS bkyn, 
                        src:bkyn_kw::varchar AS bkyn_kw, 
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bppr::varchar AS bppr, 
                        src:bppr_ref_compnr::integer AS bppr_ref_compnr, 
                        src:bptx::varchar AS bptx, 
                        src:bptx_ref_compnr::integer AS bptx_ref_compnr, 
                        src:cbrn::varchar AS cbrn, 
                        src:cbrn_ref_compnr::integer AS cbrn_ref_compnr, 
                        src:ccrs::varchar AS ccrs, 
                        src:ccrs_ref_compnr::integer AS ccrs_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:cfdt::datetime AS cfdt, 
                        src:cfrw::varchar AS cfrw, 
                        src:cfrw_ref_compnr::integer AS cfrw_ref_compnr, 
                        src:chrq::integer AS chrq, 
                        src:chrq_kw::varchar AS chrq_kw, 
                        src:clyn::integer AS clyn, 
                        src:clyn_kw::varchar AS clyn_kw, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_rcmp::integer AS cofc_rcmp, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:corg::integer AS corg, 
                        src:corg_kw::varchar AS corg_kw, 
                        src:corn::object AS corn, 
                        src:cosn::varchar AS cosn, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:cpls::varchar AS cpls, 
                        src:cpls_ref_compnr::integer AS cpls_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crby::varchar AS crby, 
                        src:crcd::varchar AS crcd, 
                        src:crcd_ref_compnr::integer AS crcd_ref_compnr, 
                        src:crdt::datetime AS crdt, 
                        src:creg::varchar AS creg, 
                        src:creg_ref_compnr::integer AS creg_ref_compnr, 
                        src:crep::varchar AS crep, 
                        src:crep_ref_compnr::integer AS crep_ref_compnr, 
                        src:crht::integer AS crht, 
                        src:crht_ref_compnr::integer AS crht_ref_compnr, 
                        src:crin::integer AS crin, 
                        src:crin_kw::varchar AS crin_kw, 
                        src:cror::integer AS cror, 
                        src:cror_kw::varchar AS cror_kw, 
                        src:crrq::integer AS crrq, 
                        src:crrq_kw::varchar AS crrq_kw, 
                        src:crst::integer AS crst, 
                        src:crst_kw::varchar AS crst_kw, 
                        src:crte::varchar AS crte, 
                        src:crte_ref_compnr::integer AS crte_ref_compnr, 
                        src:ctcd::varchar AS ctcd, 
                        src:ctcd_ref_compnr::integer AS ctcd_ref_compnr, 
                        src:cvyn::integer AS cvyn, 
                        src:cvyn_kw::varchar AS cvyn_kw, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:ddat::datetime AS ddat, 
                        src:ddtc::datetime AS ddtc, 
                        src:delc::varchar AS delc, 
                        src:delc_ref_compnr::integer AS delc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:ehis::integer AS ehis, 
                        src:ehis_kw::varchar AS ehis_kw, 
                        src:esin::integer AS esin, 
                        src:esin_kw::varchar AS esin_kw, 
                        src:fcrt::integer AS fcrt, 
                        src:fcrt_kw::varchar AS fcrt_kw, 
                        src:fdpt::varchar AS fdpt, 
                        src:fdpt_ref_compnr::integer AS fdpt_ref_compnr, 
                        src:frin::integer AS frin, 
                        src:frin_kw::varchar AS frin_kw, 
                        src:futo::integer AS futo, 
                        src:futo_kw::varchar AS futo_kw, 
                        src:hdst::integer AS hdst, 
                        src:hdst_kw::varchar AS hdst_kw, 
                        src:hism::integer AS hism, 
                        src:hism_kw::varchar AS hism_kw, 
                        src:hiss::integer AS hiss, 
                        src:hiss_kw::varchar AS hiss_kw, 
                        src:infr::integer AS infr, 
                        src:infr_kw::varchar AS infr_kw, 
                        src:inpl::varchar AS inpl, 
                        src:inpl_ref_compnr::integer AS inpl_ref_compnr, 
                        src:itad::varchar AS itad, 
                        src:itad_ref_compnr::integer AS itad_ref_compnr, 
                        src:itbp::varchar AS itbp, 
                        src:itbp_ref_compnr::integer AS itbp_ref_compnr, 
                        src:itcn::varchar AS itcn, 
                        src:itcn_ref_compnr::integer AS itcn_ref_compnr, 
                        src:lcrq::integer AS lcrq, 
                        src:lcrq_kw::varchar AS lcrq_kw, 
                        src:motv::varchar AS motv, 
                        src:motv_ref_compnr::integer AS motv_ref_compnr, 
                        src:mprm::integer AS mprm, 
                        src:mprm_kw::varchar AS mprm_kw, 
                        src:oamt::numeric(38, 17) AS oamt, 
                        src:odat::datetime AS odat, 
                        src:odis::numeric(38, 17) AS odis, 
                        src:odno::varchar AS odno, 
                        src:odty::integer AS odty, 
                        src:odty_kw::varchar AS odty_kw, 
                        src:ofad::varchar AS ofad, 
                        src:ofad_ref_compnr::integer AS ofad_ref_compnr, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:ofcn::varchar AS ofcn, 
                        src:ofcn_ref_compnr::integer AS ofcn_ref_compnr, 
                        src:oref::object AS oref, 
                        src:orno::varchar AS orno, 
                        src:orno_cosn_ref_compnr::integer AS orno_cosn_ref_compnr, 
                        src:osor::varchar AS osor, 
                        src:osrp::varchar AS osrp, 
                        src:osrp_ref_compnr::integer AS osrp_ref_compnr, 
                        src:paym::varchar AS paym, 
                        src:pcod::varchar AS pcod, 
                        src:pcod_ref_compnr::integer AS pcod_ref_compnr, 
                        src:pfad::varchar AS pfad, 
                        src:pfad_ref_compnr::integer AS pfad_ref_compnr, 
                        src:pfbp::varchar AS pfbp, 
                        src:pfbp_ref_compnr::integer AS pfbp_ref_compnr, 
                        src:pfcn::varchar AS pfcn, 
                        src:pfcn_ref_compnr::integer AS pfcn_ref_compnr, 
                        src:pldd::varchar AS pldd, 
                        src:pldd_ref_compnr::integer AS pldd_ref_compnr, 
                        src:prdt::datetime AS prdt, 
                        src:prno::varchar AS prno, 
                        src:prno_ref_compnr::integer AS prno_ref_compnr, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:ratd::datetime AS ratd, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rats_1::numeric(38, 17) AS rats_1, 
                        src:rats_2::numeric(38, 17) AS rats_2, 
                        src:rats_3::numeric(38, 17) AS rats_3, 
                        src:ratt::varchar AS ratt, 
                        src:ratt_ref_compnr::integer AS ratt_ref_compnr, 
                        src:rcmp::integer AS rcmp, 
                        src:refa::object AS refa, 
                        src:refb::object AS refb, 
                        src:retr::varchar AS retr, 
                        src:retr_ref_compnr::integer AS retr_ref_compnr, 
                        src:revn::integer AS revn, 
                        src:rtyp::varchar AS rtyp, 
                        src:ruso::integer AS ruso, 
                        src:ruso_kw::varchar AS ruso_kw, 
                        src:sbim::integer AS sbim, 
                        src:sbim_kw::varchar AS sbim_kw, 
                        src:scon::integer AS scon, 
                        src:scon_kw::varchar AS scon_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shpm::varchar AS shpm, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:sotp::varchar AS sotp, 
                        src:sotp_ref_compnr::integer AS sotp_ref_compnr, 
                        src:stad::varchar AS stad, 
                        src:stad_ref_compnr::integer AS stad_ref_compnr, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:stcn::varchar AS stcn, 
                        src:stcn_ref_compnr::integer AS stcn_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tprd::numeric(38, 17) AS tprd, 
                        src:tprd_dtwc::numeric(38, 17) AS tprd_dtwc, 
                        src:tprd_lclc::numeric(38, 17) AS tprd_lclc, 
                        src:tprd_rfrc::numeric(38, 17) AS tprd_rfrc, 
                        src:tprd_rpc1::numeric(38, 17) AS tprd_rpc1, 
                        src:tprd_rpc2::numeric(38, 17) AS tprd_rpc2, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:txtb::integer AS txtb, 
                        src:txtb_ref_compnr::integer AS txtb_ref_compnr, 
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
                                        
                src:orno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS400 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:acdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:agen), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:airp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:akcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgrb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bkyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptx_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:corg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpls_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crht), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crht_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cror), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddat), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddtc), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ehis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:esin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:futo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hdst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hism), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hiss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:infr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inpl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mprm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oamt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:odat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orno_cosn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ratd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rats_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rats_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rats_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:retr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ruso), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbim), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sotp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprd_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprd_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprd_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprd_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprd_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null