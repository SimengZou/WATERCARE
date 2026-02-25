CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC232 AS SELECT
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:cdoc::object AS cdoc, 
                        src:cfpo::integer AS cfpo, 
                        src:cfpo_kw::varchar AS cfpo_kw, 
                        src:compnr::integer AS compnr, 
                        src:cper::integer AS cper, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cptc::varchar AS cptc, 
                        src:cptc_cyea_cper_ref_compnr::integer AS cptc_cyea_cper_ref_compnr, 
                        src:cptc_ref_compnr::integer AS cptc_ref_compnr, 
                        src:cpth::varchar AS cpth, 
                        src:cpth_ref_compnr::integer AS cpth_ref_compnr, 
                        src:cpth_year_peri_ref_compnr::integer AS cpth_year_peri_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:csub::varchar AS csub, 
                        src:cuni::varchar AS cuni, 
                        src:cyea::integer AS cyea, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:loco::varchar AS loco, 
                        src:ltdt::datetime AS ltdt, 
                        src:orno::varchar AS orno, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:peri::integer AS peri, 
                        src:pono::integer AS pono, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:rgdt::datetime AS rgdt, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:task::varchar AS task, 
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
    
                        
                src:compnr,
                src:cpth,
                src:year,
                src:peri,
                src:sern,
                src:otbp,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cpth,
                src:year,
                src:peri,
                src:sern,
                src:otbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC232 as strm))