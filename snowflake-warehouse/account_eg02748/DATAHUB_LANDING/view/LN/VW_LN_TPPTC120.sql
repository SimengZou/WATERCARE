CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPTC120 AS SELECT
                        src:amoc::numeric(38, 17) AS amoc, 
                        src:amos::numeric(38, 17) AS amos, 
                        src:btdt::datetime AS btdt, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:clas::varchar AS clas, 
                        src:clas_ref_compnr::integer AS clas_ref_compnr, 
                        src:cnln::varchar AS cnln, 
                        src:cocu::varchar AS cocu, 
                        src:cocu_ref_compnr::integer AS cocu_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cona::numeric(38, 17) AS cona, 
                        src:cono::varchar AS cono, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cpva::integer AS cpva, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:dbgf::integer AS dbgf, 
                        src:dbgf_kw::varchar AS dbgf_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dfpc::integer AS dfpc, 
                        src:dfpc_kw::varchar AS dfpc_kw, 
                        src:dlvr::integer AS dlvr, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:lcta::numeric(38, 17) AS lcta, 
                        src:leng::numeric(38, 17) AS leng, 
                        src:noun::integer AS noun, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:quan::numeric(38, 17) AS quan, 
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
                        src:sacu::varchar AS sacu, 
                        src:sacu_ref_compnr::integer AS sacu_ref_compnr, 
                        src:schd::integer AS schd, 
                        src:scpf::numeric(38, 17) AS scpf, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:trts::integer AS trts, 
                        src:trts_kw::varchar AS trts_kw, 
                        src:tsrl::integer AS tsrl, 
                        src:tsrl_kw::varchar AS tsrl_kw, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:widt::numeric(38, 17) AS widt, 
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
                src:cspa,
                src:sern,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:compnr,
                src:cspa,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPTC120 as strm))