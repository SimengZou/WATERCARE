CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC250 AS SELECT
                        src:amoc::numeric(38, 17) AS amoc, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdoc::object AS cdoc, 
                        src:cdoc_ref_compnr::integer AS cdoc_ref_compnr, 
                        src:cequ::varchar AS cequ, 
                        src:cfpo::integer AS cfpo, 
                        src:cfpo_kw::varchar AS cfpo_kw, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cptc::varchar AS cptc, 
                        src:cptc_ref_compnr::integer AS cptc_ref_compnr, 
                        src:cptc_year_peri_ref_compnr::integer AS cptc_year_peri_ref_compnr, 
                        src:cptf::varchar AS cptf, 
                        src:cptf_fyea_fper_ref_compnr::integer AS cptf_fyea_fper_ref_compnr, 
                        src:cptf_ref_compnr::integer AS cptf_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:entu::varchar AS entu, 
                        src:fcrt::integer AS fcrt, 
                        src:fcrt_kw::varchar AS fcrt_kw, 
                        src:fper::integer AS fper, 
                        src:fyea::integer AS fyea, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:loco::varchar AS loco, 
                        src:ltdt::datetime AS ltdt, 
                        src:orno::varchar AS orno, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:peri::integer AS peri, 
                        src:pono::integer AS pono, 
                        src:potf::integer AS potf, 
                        src:potf_kw::varchar AS potf_kw, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:ratc::numeric(38, 17) AS ratc, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rdat::datetime AS rdat, 
                        src:rgdt::datetime AS rgdt, 
                        src:rtcc_1::numeric(38, 17) AS rtcc_1, 
                        src:rtcc_2::numeric(38, 17) AS rtcc_2, 
                        src:rtcc_3::numeric(38, 17) AS rtcc_3, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:srnb::integer AS srnb, 
                        src:teta::integer AS teta, 
                        src:teta_kw::varchar AS teta_kw, 
                        src:time::numeric(38, 17) AS time, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, 
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:cprj,
                src:compnr,
                src:sern,
                src:cequ,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:compnr,
                src:sern,
                src:cequ  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC250 as strm))