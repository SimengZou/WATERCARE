CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPIN020 AS SELECT
                        src:bpcl::varchar AS bpcl, 
                        src:bpcl_ref_compnr::integer AS bpcl_ref_compnr, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:cact::varchar AS cact, 
                        src:catv::numeric(38, 17) AS catv, 
                        src:ccrs::varchar AS ccrs, 
                        src:ccrs_ref_compnr::integer AS ccrs_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_cvat_ref_compnr::integer AS ccty_cvat_ref_compnr, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:ceno::varchar AS ceno, 
                        src:clos::integer AS clos, 
                        src:clos_kw::varchar AS clos_kw, 
                        src:cnln::varchar AS cnln, 
                        src:cnpr::varchar AS cnpr, 
                        src:cnpr_cpla_cact_ref_compnr::integer AS cnpr_cpla_cact_ref_compnr, 
                        src:cnpr_cpla_ref_compnr::integer AS cnpr_cpla_ref_compnr, 
                        src:cnpr_cpla_tact_ref_compnr::integer AS cnpr_cpla_tact_ref_compnr, 
                        src:cnpr_cspa_ref_compnr::integer AS cnpr_cspa_ref_compnr, 
                        src:cnpr_ref_compnr::integer AS cnpr_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_hbnr_cprj_ofbp_ref_compnr::integer AS cono_cnln_hbnr_cprj_ofbp_ref_compnr, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cpra::varchar AS cpra, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpra::varchar AS cprj_cpra, 
                        src:cprj_cpro::varchar AS cprj_cpro, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cpro::varchar AS cpro, 
                        src:cspa::varchar AS cspa, 
                        src:cstv::numeric(38, 17) AS cstv, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:cvyn::integer AS cvyn, 
                        src:cvyn_kw::varchar AS cvyn_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:disc::numeric(38, 17) AS disc, 
                        src:dsca::object AS dsca, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:exrs::varchar AS exrs, 
                        src:exrs_ref_compnr::integer AS exrs_ref_compnr, 
                        src:fcma::integer AS fcma, 
                        src:fcmp::integer AS fcmp, 
                        src:fins::integer AS fins, 
                        src:fins_kw::varchar AS fins_kw, 
                        src:hbai::numeric(38, 17) AS hbai, 
                        src:hbnr::integer AS hbnr, 
                        src:hbpc::numeric(38, 17) AS hbpc, 
                        src:hbyn::integer AS hbyn, 
                        src:hbyn_kw::varchar AS hbyn_kw, 
                        src:idca::integer AS idca, 
                        src:idoc::integer AS idoc, 
                        src:iiaa::numeric(38, 17) AS iiaa, 
                        src:iiaa_cntc::numeric(38, 17) AS iiaa_cntc, 
                        src:iiaa_dwhc::numeric(38, 17) AS iiaa_dwhc, 
                        src:iiaa_homc::numeric(38, 17) AS iiaa_homc, 
                        src:iiaa_prjc::numeric(38, 17) AS iiaa_prjc, 
                        src:iiaa_refc::numeric(38, 17) AS iiaa_refc, 
                        src:iiaa_rpc1::numeric(38, 17) AS iiaa_rpc1, 
                        src:iiaa_rpc2::numeric(38, 17) AS iiaa_rpc2, 
                        src:inaa::numeric(38, 17) AS inaa, 
                        src:inaa_cntc::numeric(38, 17) AS inaa_cntc, 
                        src:inaa_dwhc::numeric(38, 17) AS inaa_dwhc, 
                        src:inaa_homc::numeric(38, 17) AS inaa_homc, 
                        src:inaa_prjc::numeric(38, 17) AS inaa_prjc, 
                        src:inaa_refc::numeric(38, 17) AS inaa_refc, 
                        src:inaa_rpc1::numeric(38, 17) AS inaa_rpc1, 
                        src:inaa_rpc2::numeric(38, 17) AS inaa_rpc2, 
                        src:inia::numeric(38, 17) AS inia, 
                        src:inia_cntc::numeric(38, 17) AS inia_cntc, 
                        src:inia_dwhc::numeric(38, 17) AS inia_dwhc, 
                        src:inia_homc::numeric(38, 17) AS inia_homc, 
                        src:inia_prjc::numeric(38, 17) AS inia_prjc, 
                        src:inia_refc::numeric(38, 17) AS inia_refc, 
                        src:inia_rpc1::numeric(38, 17) AS inia_rpc1, 
                        src:inia_rpc2::numeric(38, 17) AS inia_rpc2, 
                        src:inpo::integer AS inpo, 
                        src:inpr::numeric(38, 17) AS inpr, 
                        src:insa::numeric(38, 17) AS insa, 
                        src:insa_cntc::numeric(38, 17) AS insa_cntc, 
                        src:insa_dwhc::numeric(38, 17) AS insa_dwhc, 
                        src:insa_homc::numeric(38, 17) AS insa_homc, 
                        src:insa_prjc::numeric(38, 17) AS insa_prjc, 
                        src:insa_refc::numeric(38, 17) AS insa_refc, 
                        src:insa_rpc1::numeric(38, 17) AS insa_rpc1, 
                        src:insa_rpc2::numeric(38, 17) AS insa_rpc2, 
                        src:invo::integer AS invo, 
                        src:invo_kw::varchar AS invo_kw, 
                        src:itpa::varchar AS itpa, 
                        src:ityp::varchar AS ityp, 
                        src:nins::integer AS nins, 
                        src:nper::numeric(38, 17) AS nper, 
                        src:npoi::integer AS npoi, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:paym::varchar AS paym, 
                        src:peru::datetime AS peru, 
                        src:pfdt::datetime AS pfdt, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rdat::datetime AS rdat, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sidt::datetime AS sidt, 
                        src:tact::varchar AS tact, 
                        src:tcst::integer AS tcst, 
                        src:tcst_kw::varchar AS tcst_kw, 
                        src:tiaa::numeric(38, 17) AS tiaa, 
                        src:tiaa_cntc::numeric(38, 17) AS tiaa_cntc, 
                        src:tiaa_dwhc::numeric(38, 17) AS tiaa_dwhc, 
                        src:tiaa_homc::numeric(38, 17) AS tiaa_homc, 
                        src:tiaa_prjc::numeric(38, 17) AS tiaa_prjc, 
                        src:tiaa_refc::numeric(38, 17) AS tiaa_refc, 
                        src:tiaa_rpc1::numeric(38, 17) AS tiaa_rpc1, 
                        src:tiaa_rpc2::numeric(38, 17) AS tiaa_rpc2, 
                        src:tiia::numeric(38, 17) AS tiia, 
                        src:tiia_cntc::numeric(38, 17) AS tiia_cntc, 
                        src:tiia_dwhc::numeric(38, 17) AS tiia_dwhc, 
                        src:tiia_homc::numeric(38, 17) AS tiia_homc, 
                        src:tiia_prjc::numeric(38, 17) AS tiia_prjc, 
                        src:tiia_refc::numeric(38, 17) AS tiia_refc, 
                        src:tiia_rpc1::numeric(38, 17) AS tiia_rpc1, 
                        src:tiia_rpc2::numeric(38, 17) AS tiia_rpc2, 
                        src:timestamp::datetime AS timestamp, 
                        src:tipo::integer AS tipo, 
                        src:tipr::numeric(38, 17) AS tipr, 
                        src:toin::integer AS toin, 
                        src:toin_kw::varchar AS toin_kw, 
                        src:trsl::integer AS trsl, 
                        src:trsl_kw::varchar AS trsl_kw, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
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
                                        
                src:nins,
                src:ofbp,
                src:cono,
                src:cprj,
                src:cnln,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPIN020 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:catv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clos), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_cpla_cact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_cpla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_cpla_tact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnpr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_cnln_hbnr_cprj_ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fins), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbai), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idca), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iiaa_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inaa_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inia_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:insa_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nins), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:npoi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:peru), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sidt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiaa_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tiia_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tipo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tipr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:toin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null