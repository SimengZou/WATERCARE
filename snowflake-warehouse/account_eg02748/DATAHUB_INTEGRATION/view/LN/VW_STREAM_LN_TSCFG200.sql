CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TSCFG200 AS SELECT
                        src:aloc::varchar AS aloc, 
                        src:alty::integer AS alty, 
                        src:alty_kw::varchar AS alty_kw, 
                        src:bfad::varchar AS bfad, 
                        src:bfad_ref_compnr::integer AS bfad_ref_compnr, 
                        src:bfcn::varchar AS bfcn, 
                        src:bfcn_ref_compnr::integer AS bfcn_ref_compnr, 
                        src:blag::object AS blag, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:ccha::numeric(38, 17) AS ccha, 
                        src:ccnt::varchar AS ccnt, 
                        src:ccnt_ref_compnr::integer AS ccnt_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:clot::varchar AS clot, 
                        src:clst::varchar AS clst, 
                        src:clst_ref_compnr::integer AS clst_ref_compnr, 
                        src:cmea::varchar AS cmea, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:cncd::varchar AS cncd, 
                        src:cndt::datetime AS cndt, 
                        src:cnln::varchar AS cnln, 
                        src:compnr::integer AS compnr, 
                        src:coun::numeric(38, 17) AS coun, 
                        src:cpro::varchar AS cpro, 
                        src:crtm::datetime AS crtm, 
                        src:csar::varchar AS csar, 
                        src:csar_ref_compnr::integer AS csar_ref_compnr, 
                        src:cusc::varchar AS cusc, 
                        src:cusc_ref_compnr::integer AS cusc_ref_compnr, 
                        src:cvco::integer AS cvco, 
                        src:cvco_kw::varchar AS cvco_kw, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwte::varchar AS cwte, 
                        src:cwte_ref_compnr::integer AS cwte_ref_compnr, 
                        src:czon::varchar AS czon, 
                        src:czon_ref_compnr::integer AS czon_ref_compnr, 
                        src:dbpo::integer AS dbpo, 
                        src:dbpo_kw::varchar AS dbpo_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dlad::varchar AS dlad, 
                        src:dlad_ref_compnr::integer AS dlad_ref_compnr, 
                        src:dlcn::varchar AS dlcn, 
                        src:dlcn_ref_compnr::integer AS dlcn_ref_compnr, 
                        src:dler::varchar AS dler, 
                        src:dler_ref_compnr::integer AS dler_ref_compnr, 
                        src:dltm::datetime AS dltm, 
                        src:dscb::object AS dscb, 
                        src:dscc::object AS dscc, 
                        src:dscd::object AS dscd, 
                        src:dsnr::integer AS dsnr, 
                        src:dsnr_kw::varchar AS dsnr_kw, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:gpac::integer AS gpac, 
                        src:gpac_kw::varchar AS gpac_kw, 
                        src:gwte::varchar AS gwte, 
                        src:gwte_ref_compnr::integer AS gwte_ref_compnr, 
                        src:imag::varchar AS imag, 
                        src:item::varchar AS item, 
                        src:item_lste_ref_compnr::integer AS item_lste_ref_compnr, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:item_rste_ref_compnr::integer AS item_rste_ref_compnr, 
                        src:item_sern_ref_compnr::integer AS item_sern_ref_compnr, 
                        src:ladr::varchar AS ladr, 
                        src:ladr_ref_compnr::integer AS ladr_ref_compnr, 
                        src:linf::object AS linf, 
                        src:lpnr::object AS lpnr, 
                        src:lste::varchar AS lste, 
                        src:lste_ref_compnr::integer AS lste_ref_compnr, 
                        src:lutm::datetime AS lutm, 
                        src:mcno::varchar AS mcno, 
                        src:mcnr::varchar AS mcnr, 
                        src:mdpt::varchar AS mdpt, 
                        src:mdpt_ref_compnr::integer AS mdpt_ref_compnr, 
                        src:mftm::datetime AS mftm, 
                        src:mobl::integer AS mobl, 
                        src:mobl_kw::varchar AS mobl_kw, 
                        src:ofad::varchar AS ofad, 
                        src:ofad_ref_compnr::integer AS ofad_ref_compnr, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:ofcn::varchar AS ofcn, 
                        src:ofcn_ref_compnr::integer AS ofcn_ref_compnr, 
                        src:optm::datetime AS optm, 
                        src:ortp::integer AS ortp, 
                        src:ortp_kw::varchar AS ortp_kw, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:owtp::integer AS owtp, 
                        src:owtp_kw::varchar AS owtp_kw, 
                        src:pbsm::integer AS pbsm, 
                        src:pbsm_kw::varchar AS pbsm_kw, 
                        src:pelt::varchar AS pelt, 
                        src:phyt::integer AS phyt, 
                        src:phyt_kw::varchar AS phyt_kw, 
                        src:pltm::datetime AS pltm, 
                        src:pmsc::varchar AS pmsc, 
                        src:pmsc_ref_compnr::integer AS pmsc_ref_compnr, 
                        src:prfb::varchar AS prfb, 
                        src:prfb_ref_compnr::integer AS prfb_ref_compnr, 
                        src:prio::integer AS prio, 
                        src:prio_ref_compnr::integer AS prio_ref_compnr, 
                        src:prip::numeric(38, 17) AS prip, 
                        src:prop::varchar AS prop, 
                        src:prop_ref_compnr::integer AS prop_ref_compnr, 
                        src:pste::varchar AS pste, 
                        src:pste_ref_compnr::integer AS pste_ref_compnr, 
                        src:ract::varchar AS ract, 
                        src:ract_ref_compnr::integer AS ract_ref_compnr, 
                        src:revi::varchar AS revi, 
                        src:rnyn::integer AS rnyn, 
                        src:rnyn_kw::varchar AS rnyn_kw, 
                        src:rowc::integer AS rowc, 
                        src:rowc_ref_compnr::integer AS rowc_ref_compnr, 
                        src:rown::varchar AS rown, 
                        src:rown_ref_compnr::integer AS rown_ref_compnr, 
                        src:rste::varchar AS rste, 
                        src:rste_ref_compnr::integer AS rste_ref_compnr, 
                        src:rwdu::integer AS rwdu, 
                        src:rwun::integer AS rwun, 
                        src:rwun_kw::varchar AS rwun_kw, 
                        src:sbct::varchar AS sbct, 
                        src:sbct_ref_compnr::integer AS sbct_ref_compnr, 
                        src:sdno::integer AS sdno, 
                        src:sear::object AS sear, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::varchar AS sern, 
                        src:sfbp::varchar AS sfbp, 
                        src:sfbp_ref_compnr::integer AS sfbp_ref_compnr, 
                        src:shad::varchar AS shad, 
                        src:shad_ref_compnr::integer AS shad_ref_compnr, 
                        src:sigr::varchar AS sigr, 
                        src:sigr_ref_compnr::integer AS sigr_ref_compnr, 
                        src:soff::varchar AS soff, 
                        src:soff_ref_compnr::integer AS soff_ref_compnr, 
                        src:spfa::object AS spfa, 
                        src:spfb::object AS spfb, 
                        src:spno::integer AS spno, 
                        src:srbp::object AS srbp, 
                        src:srno::varchar AS srno, 
                        src:sswt::integer AS sswt, 
                        src:sswt_kw::varchar AS sswt_kw, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:strm::integer AS strm, 
                        src:sttm::datetime AS sttm, 
                        src:sttp::integer AS sttp, 
                        src:sttp_kw::varchar AS sttp_kw, 
                        src:swte::varchar AS swte, 
                        src:swte_ref_compnr::integer AS swte_ref_compnr, 
                        src:term::integer AS term, 
                        src:tery::varchar AS tery, 
                        src:tery_ref_compnr::integer AS tery_ref_compnr, 
                        src:tgps::integer AS tgps, 
                        src:tgps_kw::varchar AS tgps_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:titm::varchar AS titm, 
                        src:titm_ref_compnr::integer AS titm_ref_compnr, 
                        src:titm_tser_ref_compnr::integer AS titm_tser_ref_compnr, 
                        src:trdt::numeric(38, 17) AS trdt, 
                        src:trtm::numeric(38, 17) AS trtm, 
                        src:tser::varchar AS tser, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:txtb::integer AS txtb, 
                        src:txtb_ref_compnr::integer AS txtb_ref_compnr, 
                        src:txtc::integer AS txtc, 
                        src:txtc_ref_compnr::integer AS txtc_ref_compnr, 
                        src:ubad::varchar AS ubad, 
                        src:ubad_ref_compnr::integer AS ubad_ref_compnr, 
                        src:ubbp::varchar AS ubbp, 
                        src:ubbp_ref_compnr::integer AS ubbp_ref_compnr, 
                        src:ubcn::varchar AS ubcn, 
                        src:ubcn_ref_compnr::integer AS ubcn_ref_compnr, 
                        src:ubop::varchar AS ubop, 
                        src:ubop_ref_compnr::integer AS ubop_ref_compnr, 
                        src:ubpc::integer AS ubpc, 
                        src:ubpc_kw::varchar AS ubpc_kw, 
                        src:username::varchar AS username, 
                        src:wcel::varchar AS wcel, 
                        src:wght::numeric(38, 17) AS wght, 
                        src:wstt::varchar AS wstt, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:sern,
                src:compnr,
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCFG200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccha), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cndt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coun), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crtm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cusc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:czon_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dlad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dlcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dler_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dltm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gpac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gwte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_lste_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_rste_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ladr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lste_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lutm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mdpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:mftm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mobl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:optm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ortp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pbsm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phyt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pltm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmsc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prfb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prio), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prip), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prop_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pste_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rnyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rowc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rwdu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rwun), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbct_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sdno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sigr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:soff_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sswt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:strm), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sttm), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sttp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:swte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:term), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tery_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tgps), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:titm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:titm_tser_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trdt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trtm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubcn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubop_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wght), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null