CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH220 AS SELECT
                        src:acpn::integer AS acpn, 
                        src:acpn_kw::varchar AS acpn_kw, 
                        src:acvt::integer AS acvt, 
                        src:acvt_kw::varchar AS acvt_kw, 
                        src:addt::datetime AS addt, 
                        src:adin::varchar AS adin, 
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:bcko::integer AS bcko, 
                        src:bcko_kw::varchar AS bcko_kw, 
                        src:bflh::integer AS bflh, 
                        src:bflh_kw::varchar AS bflh_kw, 
                        src:bgxc::integer AS bgxc, 
                        src:bgxc_kw::varchar AS bgxc_kw, 
                        src:blck::integer AS blck, 
                        src:blck_kw::varchar AS blck_kw, 
                        src:casi::varchar AS casi, 
                        src:casi_ref_compnr::integer AS casi_ref_compnr, 
                        src:cdck::integer AS cdck, 
                        src:cdck_kw::varchar AS cdck_kw, 
                        src:circ::object AS circ, 
                        src:clot::varchar AS clot, 
                        src:cncl::integer AS cncl, 
                        src:cncl_kw::varchar AS cncl_kw, 
                        src:comp::integer AS comp, 
                        src:compnr::integer AS compnr, 
                        src:corn::object AS corn, 
                        src:csvc::varchar AS csvc, 
                        src:csvc_ref_compnr::integer AS csvc_ref_compnr, 
                        src:csvl::numeric(38, 17) AS csvl, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dlpt::varchar AS dlpt, 
                        src:dtag::varchar AS dtag, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:fact::varchar AS fact, 
                        src:fcco::varchar AS fcco, 
                        src:fcco_ref_compnr::integer AS fcco_ref_compnr, 
                        src:fisc::integer AS fisc, 
                        src:fisc_kw::varchar AS fisc_kw, 
                        src:fmln::integer AS fmln, 
                        src:fmor::varchar AS fmor, 
                        src:fprj::varchar AS fprj, 
                        src:fprj_ref_compnr::integer AS fprj_ref_compnr, 
                        src:fspa::varchar AS fspa, 
                        src:fstl::varchar AS fstl, 
                        src:gefo::integer AS gefo, 
                        src:gefo_kw::varchar AS gefo_kw, 
                        src:gnld::varchar AS gnld, 
                        src:hstq::integer AS hstq, 
                        src:hstq_kw::varchar AS hstq_kw, 
                        src:huid::varchar AS huid, 
                        src:huwt::integer AS huwt, 
                        src:huwt_kw::varchar AS huwt_kw, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:imcs::varchar AS imcs, 
                        src:inup::integer AS inup, 
                        src:inup_kw::varchar AS inup_kw, 
                        src:iown::integer AS iown, 
                        src:iown_kw::varchar AS iown_kw, 
                        src:ipay::integer AS ipay, 
                        src:ipay_kw::varchar AS ipay_kw, 
                        src:iscn::integer AS iscn, 
                        src:iscn_kw::varchar AS iscn_kw, 
                        src:istr::integer AS istr, 
                        src:istr_kw::varchar AS istr_kw, 
                        src:item::varchar AS item, 
                        src:item_atse_ref_compnr::integer AS item_atse_ref_compnr, 
                        src:item_clot_ref_compnr::integer AS item_clot_ref_compnr, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:iubw::integer AS iubw, 
                        src:iubw_kw::varchar AS iubw_kw, 
                        src:lsel::integer AS lsel, 
                        src:lsel_kw::varchar AS lsel_kw, 
                        src:lsta::integer AS lsta, 
                        src:lsta_kw::varchar AS lsta_kw, 
                        src:masp::integer AS masp, 
                        src:masp_kw::varchar AS masp_kw, 
                        src:oarq::integer AS oarq, 
                        src:oarq_kw::varchar AS oarq_kw, 
                        src:ocur::varchar AS ocur, 
                        src:ocur_ref_compnr::integer AS ocur_ref_compnr, 
                        src:oorg::integer AS oorg, 
                        src:oorg_kw::varchar AS oorg_kw, 
                        src:oorg_orno_oset_ref_compnr::integer AS oorg_orno_oset_ref_compnr, 
                        src:orno::varchar AS orno, 
                        src:orpr::numeric(38, 17) AS orpr, 
                        src:orun::varchar AS orun, 
                        src:orun_ref_compnr::integer AS orun_ref_compnr, 
                        src:oset::integer AS oset, 
                        src:ovfp::integer AS ovfp, 
                        src:ovfp_kw::varchar AS ovfp_kw, 
                        src:ovlp::integer AS ovlp, 
                        src:ovlp_kw::varchar AS ovlp_kw, 
                        src:owns::integer AS owns, 
                        src:owns_kw::varchar AS owns_kw, 
                        src:paym::integer AS paym, 
                        src:paym_kw::varchar AS paym_kw, 
                        src:pddt::datetime AS pddt, 
                        src:pkdb::integer AS pkdb, 
                        src:pkdb_kw::varchar AS pkdb_kw, 
                        src:pkdf::varchar AS pkdf, 
                        src:pkdf_ref_compnr::integer AS pkdf_ref_compnr, 
                        src:pono::integer AS pono, 
                        src:prdt::datetime AS prdt, 
                        src:prio::integer AS prio, 
                        src:prjp::integer AS prjp, 
                        src:prjp_kw::varchar AS prjp_kw, 
                        src:qacp::numeric(38, 17) AS qacp, 
                        src:qact::numeric(38, 17) AS qact, 
                        src:qadp::numeric(38, 17) AS qadp, 
                        src:qadv::numeric(38, 17) AS qadv, 
                        src:qapp::numeric(38, 17) AS qapp, 
                        src:qapr::numeric(38, 17) AS qapr, 
                        src:qcad::numeric(38, 17) AS qcad, 
                        src:qcap::numeric(38, 17) AS qcap, 
                        src:qcnl::numeric(38, 17) AS qcnl, 
                        src:qcnp::numeric(38, 17) AS qcnp, 
                        src:qnpu::numeric(38, 17) AS qnpu, 
                        src:qnse::numeric(38, 17) AS qnse, 
                        src:qnsh::numeric(38, 17) AS qnsh, 
                        src:qnsp::numeric(38, 17) AS qnsp, 
                        src:qoop::numeric(38, 17) AS qoop, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:qopu::numeric(38, 17) AS qopu, 
                        src:qord::numeric(38, 17) AS qord, 
                        src:qoro::numeric(38, 17) AS qoro, 
                        src:qorp::numeric(38, 17) AS qorp, 
                        src:qova::numeric(38, 17) AS qova, 
                        src:qovd::numeric(38, 17) AS qovd, 
                        src:qovp::numeric(38, 17) AS qovp, 
                        src:qpic::numeric(38, 17) AS qpic, 
                        src:qpip::numeric(38, 17) AS qpip, 
                        src:qprv::numeric(38, 17) AS qprv, 
                        src:qpss::numeric(38, 17) AS qpss, 
                        src:qpsv::numeric(38, 17) AS qpsv, 
                        src:qrej::numeric(38, 17) AS qrej, 
                        src:qrel::numeric(38, 17) AS qrel, 
                        src:qrep::numeric(38, 17) AS qrep, 
                        src:qreq::numeric(38, 17) AS qreq, 
                        src:qrpu::numeric(38, 17) AS qrpu, 
                        src:qrsc::numeric(38, 17) AS qrsc, 
                        src:qrsp::numeric(38, 17) AS qrsp, 
                        src:qscl::numeric(38, 17) AS qscl, 
                        src:qscp::numeric(38, 17) AS qscp, 
                        src:qshp::numeric(38, 17) AS qshp, 
                        src:qspu::numeric(38, 17) AS qspu, 
                        src:refe::object AS refe, 
                        src:refs::object AS refs, 
                        src:revi::varchar AS revi, 
                        src:rorg::integer AS rorg, 
                        src:rorg_kw::varchar AS rorg_kw, 
                        src:rorn::varchar AS rorn, 
                        src:rowc::integer AS rowc, 
                        src:rowc_ref_compnr::integer AS rowc_ref_compnr, 
                        src:rown::varchar AS rown, 
                        src:rown_ref_compnr::integer AS rown_ref_compnr, 
                        src:rpon::integer AS rpon, 
                        src:rqpu::numeric(38, 17) AS rqpu, 
                        src:rseq::integer AS rseq, 
                        src:rush::integer AS rush, 
                        src:rush_kw::varchar AS rush_kw, 
                        src:scon::integer AS scon, 
                        src:scon_kw::varchar AS scon_kw, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serl::varchar AS serl, 
                        src:shrt::integer AS shrt, 
                        src:shrt_kw::varchar AS shrt_kw, 
                        src:spcn::varchar AS spcn, 
                        src:ssts::integer AS ssts, 
                        src:ssts_kw::varchar AS ssts_kw, 
                        src:stad::varchar AS stad, 
                        src:stad_dlpt_ref_compnr::integer AS stad_dlpt_ref_compnr, 
                        src:sups::integer AS sups, 
                        src:sups_kw::varchar AS sups_kw, 
                        src:tact::varchar AS tact, 
                        src:tcco::varchar AS tcco, 
                        src:tcco_ref_compnr::integer AS tcco_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tprj::varchar AS tprj, 
                        src:tprj_ref_compnr::integer AS tprj_ref_compnr, 
                        src:tspa::varchar AS tspa, 
                        src:tstl::varchar AS tstl, 
                        src:txtn::integer AS txtn, 
                        src:txtn_ref_compnr::integer AS txtn_ref_compnr, 
                        src:ubin::integer AS ubin, 
                        src:ubin_kw::varchar AS ubin_kw, 
                        src:username::varchar AS username, 
                        src:uwop::integer AS uwop, 
                        src:uwop_kw::varchar AS uwop_kw, 
                        src:wmss::integer AS wmss, 
                        src:wmss_kw::varchar AS wmss_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:seqn,
                src:pono,
                src:compnr,
                src:oorg  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH220 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acpn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acvt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:addt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bcko), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bflh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgxc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blck), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdck), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cncl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fisc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fmln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gefo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hstq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:huwt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inup), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iown), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipay), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iscn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:istr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_atse_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_clot_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iubw), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lsel), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lsta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:masp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oarq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ocur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oorg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oorg_orno_oset_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orun_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oset), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ovfp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ovlp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:owns), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:paym), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pddt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pkdb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prio), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prjp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qacp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qact), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qadp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qadv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qapp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qapr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qcad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qcap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qcnl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qcnp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qnpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qnse), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qnsh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qnsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoop), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qopu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qord), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoro), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qorp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qova), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qovd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qovp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpip), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qprv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpsv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrej), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrel), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrep), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qreq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrsc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qscl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qscp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qshp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qspu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rorg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rowc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rqpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rush), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ssts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sups), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uwop), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wmss), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null