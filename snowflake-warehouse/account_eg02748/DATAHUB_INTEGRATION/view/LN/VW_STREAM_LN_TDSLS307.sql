CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDSLS307 AS SELECT
                        src:adin::varchar AS adin, 
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:casi::varchar AS casi, 
                        src:casi_ref_compnr::integer AS casi_ref_compnr, 
                        src:cchn::varchar AS cchn, 
                        src:cchp::varchar AS cchp, 
                        src:ccof::varchar AS ccof, 
                        src:ccof_ref_compnr::integer AS ccof_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_cvat_ref_compnr::integer AS ccty_cvat_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:ceno::varchar AS ceno, 
                        src:cfdt::datetime AS cfdt, 
                        src:cfrw::varchar AS cfrw, 
                        src:cfrw_ref_compnr::integer AS cfrw_ref_compnr, 
                        src:chan::varchar AS chan, 
                        src:chan_ref_compnr::integer AS chan_ref_compnr, 
                        src:citr::object AS citr, 
                        src:clot::varchar AS clot, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_pono_ccof_ref_compnr::integer AS cono_pono_ccof_ref_compnr, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:crdt::datetime AS crdt, 
                        src:ctyp::integer AS ctyp, 
                        src:ctyp_kw::varchar AS ctyp_kw, 
                        src:cucf::varchar AS cucf, 
                        src:cucf_ref_compnr::integer AS cucf_ref_compnr, 
                        src:cups::varchar AS cups, 
                        src:cuqs::varchar AS cuqs, 
                        src:cuqs_ref_compnr::integer AS cuqs_ref_compnr, 
                        src:curq::varchar AS curq, 
                        src:curq_ref_compnr::integer AS curq_ref_compnr, 
                        src:cuva::numeric(38, 17) AS cuva, 
                        src:cvat::varchar AS cvat, 
                        src:cvcf::numeric(38, 17) AS cvcf, 
                        src:cvps::numeric(38, 17) AS cvps, 
                        src:cvqs::numeric(38, 17) AS cvqs, 
                        src:cvrq::numeric(38, 17) AS cvrq, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:ddta::datetime AS ddta, 
                        src:deleted::boolean AS deleted, 
                        src:dest::integer AS dest, 
                        src:dest_kw::varchar AS dest_kw, 
                        src:dlno::varchar AS dlno, 
                        src:dlpt::varchar AS dlpt, 
                        src:dock::varchar AS dock, 
                        src:edat::datetime AS edat, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:gamt_dtwc::numeric(38, 17) AS gamt_dtwc, 
                        src:gamt_lclc::numeric(38, 17) AS gamt_lclc, 
                        src:gamt_rfrc::numeric(38, 17) AS gamt_rfrc, 
                        src:gamt_rpc1::numeric(38, 17) AS gamt_rpc1, 
                        src:gamt_rpc2::numeric(38, 17) AS gamt_rpc2, 
                        src:gamt_trnc::numeric(38, 17) AS gamt_trnc, 
                        src:hold::integer AS hold, 
                        src:hold_kw::varchar AS hold_kw, 
                        src:imcs::varchar AS imcs, 
                        src:invd::datetime AS invd, 
                        src:invn::integer AS invn, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:item_site_ref_compnr::integer AS item_site_ref_compnr, 
                        src:item_stsi_ref_compnr::integer AS item_stsi_ref_compnr, 
                        src:leng::numeric(38, 17) AS leng, 
                        src:lnor::integer AS lnor, 
                        src:lnor_kw::varchar AS lnor_kw, 
                        src:namt_dtwc::numeric(38, 17) AS namt_dtwc, 
                        src:namt_lclc::numeric(38, 17) AS namt_lclc, 
                        src:namt_rfrc::numeric(38, 17) AS namt_rfrc, 
                        src:namt_rpc1::numeric(38, 17) AS namt_rpc1, 
                        src:namt_rpc2::numeric(38, 17) AS namt_rpc2, 
                        src:namt_trnc::numeric(38, 17) AS namt_trnc, 
                        src:odat::datetime AS odat, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:opon::integer AS opon, 
                        src:orno::varchar AS orno, 
                        src:pmnt::integer AS pmnt, 
                        src:pmnt_kw::varchar AS pmnt_kw, 
                        src:pono::integer AS pono, 
                        src:prdt::datetime AS prdt, 
                        src:prfa::object AS prfa, 
                        src:prfb::object AS prfb, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:prid::varchar AS prid, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:qccf::numeric(38, 17) AS qccf, 
                        src:qccf_buar::numeric(38, 17) AS qccf_buar, 
                        src:qccf_buln::numeric(38, 17) AS qccf_buln, 
                        src:qccf_bupc::numeric(38, 17) AS qccf_bupc, 
                        src:qccf_butm::numeric(38, 17) AS qccf_butm, 
                        src:qccf_buvl::numeric(38, 17) AS qccf_buvl, 
                        src:qccf_buwg::numeric(38, 17) AS qccf_buwg, 
                        src:qccf_invu::numeric(38, 17) AS qccf_invu, 
                        src:qccf_trnu::numeric(38, 17) AS qccf_trnu, 
                        src:qidl::numeric(38, 17) AS qidl, 
                        src:qidl_buar::numeric(38, 17) AS qidl_buar, 
                        src:qidl_buln::numeric(38, 17) AS qidl_buln, 
                        src:qidl_bupc::numeric(38, 17) AS qidl_bupc, 
                        src:qidl_butm::numeric(38, 17) AS qidl_butm, 
                        src:qidl_buvl::numeric(38, 17) AS qidl_buvl, 
                        src:qidl_buwg::numeric(38, 17) AS qidl_buwg, 
                        src:qidl_trnu::numeric(38, 17) AS qidl_trnu, 
                        src:qoad::numeric(38, 17) AS qoad, 
                        src:qoad_buar::numeric(38, 17) AS qoad_buar, 
                        src:qoad_buln::numeric(38, 17) AS qoad_buln, 
                        src:qoad_bupc::numeric(38, 17) AS qoad_bupc, 
                        src:qoad_butm::numeric(38, 17) AS qoad_butm, 
                        src:qoad_buvl::numeric(38, 17) AS qoad_buvl, 
                        src:qoad_buwg::numeric(38, 17) AS qoad_buwg, 
                        src:qoad_invu::numeric(38, 17) AS qoad_invu, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:qoor_buar::numeric(38, 17) AS qoor_buar, 
                        src:qoor_buln::numeric(38, 17) AS qoor_buln, 
                        src:qoor_bupc::numeric(38, 17) AS qoor_bupc, 
                        src:qoor_butm::numeric(38, 17) AS qoor_butm, 
                        src:qoor_buvl::numeric(38, 17) AS qoor_buvl, 
                        src:qoor_buwg::numeric(38, 17) AS qoor_buwg, 
                        src:qoor_invu::numeric(38, 17) AS qoor_invu, 
                        src:qrrq::numeric(38, 17) AS qrrq, 
                        src:qrrq_buar::numeric(38, 17) AS qrrq_buar, 
                        src:qrrq_buln::numeric(38, 17) AS qrrq_buln, 
                        src:qrrq_bupc::numeric(38, 17) AS qrrq_bupc, 
                        src:qrrq_butm::numeric(38, 17) AS qrrq_butm, 
                        src:qrrq_buvl::numeric(38, 17) AS qrrq_buvl, 
                        src:qrrq_buwg::numeric(38, 17) AS qrrq_buwg, 
                        src:qrrq_invu::numeric(38, 17) AS qrrq_invu, 
                        src:qrrq_trnu::numeric(38, 17) AS qrrq_trnu, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:refe::object AS refe, 
                        src:refs::object AS refs, 
                        src:revi::varchar AS revi, 
                        src:revn::integer AS revn, 
                        src:rrdt::datetime AS rrdt, 
                        src:rtyp::integer AS rtyp, 
                        src:rtyp_kw::varchar AS rtyp_kw, 
                        src:samt::numeric(38, 17) AS samt, 
                        src:sbim::integer AS sbim, 
                        src:sbim_kw::varchar AS sbim_kw, 
                        src:schn::varchar AS schn, 
                        src:schn_sctp_revn_ref_compnr::integer AS schn_sctp_revn_ref_compnr, 
                        src:scmp::integer AS scmp, 
                        src:sctp::integer AS sctp, 
                        src:sctp_kw::varchar AS sctp_kw, 
                        src:sdat::datetime AS sdat, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:smsl::integer AS smsl, 
                        src:smsl_kw::varchar AS smsl_kw, 
                        src:spcn::varchar AS spcn, 
                        src:spon::integer AS spon, 
                        src:stad::varchar AS stad, 
                        src:stad_dlpt_ref_compnr::integer AS stad_dlpt_ref_compnr, 
                        src:stad_ref_compnr::integer AS stad_ref_compnr, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:stsi::varchar AS stsi, 
                        src:stsi_ref_compnr::integer AS stsi_ref_compnr, 
                        src:stwh::varchar AS stwh, 
                        src:stwh_ref_compnr::integer AS stwh_ref_compnr, 
                        src:styp::varchar AS styp, 
                        src:styp_ref_compnr::integer AS styp_ref_compnr, 
                        src:tcpr_dtwc::numeric(38, 17) AS tcpr_dtwc, 
                        src:tcpr_lclc::numeric(38, 17) AS tcpr_lclc, 
                        src:tcpr_rfrc::numeric(38, 17) AS tcpr_rfrc, 
                        src:tcpr_rpc1::numeric(38, 17) AS tcpr_rpc1, 
                        src:tcpr_rpc2::numeric(38, 17) AS tcpr_rpc2, 
                        src:tcpr_trnc::numeric(38, 17) AS tcpr_trnc, 
                        src:thic::numeric(38, 17) AS thic, 
                        src:timestamp::datetime AS timestamp, 
                        src:tiun::integer AS tiun, 
                        src:tiun_kw::varchar AS tiun_kw, 
                        src:ttyp::varchar AS ttyp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
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
                                        
                src:revn,
                src:compnr,
                src:schn,
                src:sctp,
                src:spon  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS307 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccof_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfrw_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_pono_ccof_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cucf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:curq_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuva), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvcf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvps), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvqs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddta), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dest), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:edat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gamt_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gamt_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gamt_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gamt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gamt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gamt_trnc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hold), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:invd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_stsi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:leng), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lnor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:namt_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:namt_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:namt_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:namt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:namt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:namt_trnc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:odat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qccf_trnu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl_trnu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoad_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qoor_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrrq_trnu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revn), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rrdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:samt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbim), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:schn_sctp_revn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sctp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stad_dlpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stwh_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:styp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcpr_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcpr_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcpr_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcpr_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcpr_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcpr_trnc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:thic), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiun), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:widt), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null