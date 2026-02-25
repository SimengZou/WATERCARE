CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC301 AS SELECT
                        src:amhc_1::numeric(38, 17) AS amhc_1, 
                        src:amhc_2::numeric(38, 17) AS amhc_2, 
                        src:amhc_3::numeric(38, 17) AS amhc_3, 
                        src:amor::numeric(38, 17) AS amor, 
                        src:amor_cntc::numeric(38, 17) AS amor_cntc, 
                        src:amor_dwhc::numeric(38, 17) AS amor_dwhc, 
                        src:amor_homc::numeric(38, 17) AS amor_homc, 
                        src:amor_prjc::numeric(38, 17) AS amor_prjc, 
                        src:amor_refc::numeric(38, 17) AS amor_refc, 
                        src:amor_rpc1::numeric(38, 17) AS amor_rpc1, 
                        src:amor_rpc2::numeric(38, 17) AS amor_rpc2, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdoc::object AS cdoc, 
                        src:ceno::varchar AS ceno, 
                        src:cfpo::integer AS cfpo, 
                        src:cfpo_kw::varchar AS cfpo_kw, 
                        src:cnln::varchar AS cnln, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cpro::varchar AS cprj_cpro, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cpro::varchar AS cpro, 
                        src:cptc::varchar AS cptc, 
                        src:cptc_ref_compnr::integer AS cptc_ref_compnr, 
                        src:cptc_year_peri_ref_compnr::integer AS cptc_year_peri_ref_compnr, 
                        src:cptf::varchar AS cptf, 
                        src:cptf_ref_compnr::integer AS cptf_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:cstv::numeric(38, 17) AS cstv, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dlvr::integer AS dlvr, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:entu::varchar AS entu, 
                        src:exmt::integer AS exmt, 
                        src:exmt_kw::varchar AS exmt_kw, 
                        src:exrs::varchar AS exrs, 
                        src:exrs_ref_compnr::integer AS exrs_ref_compnr, 
                        src:fcmp::integer AS fcmp, 
                        src:fcrt::integer AS fcrt, 
                        src:fcrt_kw::varchar AS fcrt_kw, 
                        src:fdoc::integer AS fdoc, 
                        src:flin::integer AS flin, 
                        src:fper::integer AS fper, 
                        src:fsrl::integer AS fsrl, 
                        src:ftln::integer AS ftln, 
                        src:ftty::varchar AS ftty, 
                        src:fyea::integer AS fyea, 
                        src:guid::varchar AS guid, 
                        src:hbnr::integer AS hbnr, 
                        src:idoc::integer AS idoc, 
                        src:invt::integer AS invt, 
                        src:invt_kw::varchar AS invt_kw, 
                        src:itbp::varchar AS itbp, 
                        src:itbp_ref_compnr::integer AS itbp_ref_compnr, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:ityp::varchar AS ityp, 
                        src:loco::varchar AS loco, 
                        src:ltdt::datetime AS ltdt, 
                        src:nadv::integer AS nadv, 
                        src:ncmp::integer AS ncmp, 
                        src:nins::integer AS nins, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:peri::integer AS peri, 
                        src:potf::integer AS potf, 
                        src:potf_kw::varchar AS potf_kw, 
                        src:prdt::datetime AS prdt, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rdat::datetime AS rdat, 
                        src:rest::integer AS rest, 
                        src:rest_kw::varchar AS rest_kw, 
                        src:rgdt::datetime AS rgdt, 
                        src:rper::integer AS rper, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:ryea::integer AS ryea, 
                        src:schd::integer AS schd, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:tetm::integer AS tetm, 
                        src:tetm_kw::varchar AS tetm_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
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
                src:cprj,
                src:sern,
                src:cpro  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC301 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amhc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amhc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amhc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_prjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_cnln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dlvr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:exrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fsrl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ftln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fyea), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nadv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nins), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:peri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:potf), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rest), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rgdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ryea), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:schd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tetm), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null