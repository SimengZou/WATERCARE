CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC256 AS SELECT
                        src:amoc::numeric(38, 17) AS amoc, 
                        src:amoc_cntc::numeric(38, 17) AS amoc_cntc, 
                        src:amoc_dwhc::numeric(38, 17) AS amoc_dwhc, 
                        src:amoc_homc::numeric(38, 17) AS amoc_homc, 
                        src:amoc_prjc::numeric(38, 17) AS amoc_prjc, 
                        src:amoc_refc::numeric(38, 17) AS amoc_refc, 
                        src:amoc_rpc1::numeric(38, 17) AS amoc_rpc1, 
                        src:amoc_rpc2::numeric(38, 17) AS amoc_rpc2, 
                        src:appr::integer AS appr, 
                        src:appr_kw::varchar AS appr_kw, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cequ::varchar AS cequ, 
                        src:cequ_rcmp::integer AS cequ_rcmp, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cequ::varchar AS cprj_cequ, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_fcmp::integer AS cprj_fcmp, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:feac_cntc::numeric(38, 17) AS feac_cntc, 
                        src:feac_dwhc::numeric(38, 17) AS feac_dwhc, 
                        src:feac_homc::numeric(38, 17) AS feac_homc, 
                        src:feac_prjc::numeric(38, 17) AS feac_prjc, 
                        src:feac_refc::numeric(38, 17) AS feac_refc, 
                        src:feac_rpc1::numeric(38, 17) AS feac_rpc1, 
                        src:feac_rpc2::numeric(38, 17) AS feac_rpc2, 
                        src:feac_trnc::numeric(38, 17) AS feac_trnc, 
                        src:frat::integer AS frat, 
                        src:frat_kw::varchar AS frat_kw, 
                        src:pric_cntc::numeric(38, 17) AS pric_cntc, 
                        src:pric_dwhc::numeric(38, 17) AS pric_dwhc, 
                        src:pric_homc::numeric(38, 17) AS pric_homc, 
                        src:pric_prjc::numeric(38, 17) AS pric_prjc, 
                        src:pric_refc::numeric(38, 17) AS pric_refc, 
                        src:pric_rpc1::numeric(38, 17) AS pric_rpc1, 
                        src:pric_rpc2::numeric(38, 17) AS pric_rpc2, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:quan_buar::numeric(38, 17) AS quan_buar, 
                        src:quan_buln::numeric(38, 17) AS quan_buln, 
                        src:quan_bupc::numeric(38, 17) AS quan_bupc, 
                        src:quan_buvl::numeric(38, 17) AS quan_buvl, 
                        src:quan_buwg::numeric(38, 17) AS quan_buwg, 
                        src:ratc::numeric(38, 17) AS ratc, 
                        src:rdat::datetime AS rdat, 
                        src:rgdt::datetime AS rgdt, 
                        src:rtcc_1::numeric(38, 17) AS rtcc_1, 
                        src:rtcc_2::numeric(38, 17) AS rtcc_2, 
                        src:rtcc_3::numeric(38, 17) AS rtcc_3, 
                        src:rtfc_1::integer AS rtfc_1, 
                        src:rtfc_2::integer AS rtfc_2, 
                        src:rtfc_3::integer AS rtfc_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:time::numeric(38, 17) AS time, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
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
                src:cspa,
                src:rgdt,
                src:compnr,
                src:cequ,
                src:cact,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:cspa,
                src:rgdt,
                src:compnr,
                src:cequ,
                src:cact  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC256 as strm))