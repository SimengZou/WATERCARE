CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPTC147 AS SELECT
                        src:amoc::numeric(38, 17) AS amoc, 
                        src:bsqn::integer AS bsqn, 
                        src:btdt::datetime AS btdt, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:cceq::varchar AS cceq, 
                        src:cequ::varchar AS cequ, 
                        src:cocu::varchar AS cocu, 
                        src:cocu_ref_compnr::integer AS cocu_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:deleted::boolean AS deleted, 
                        src:dfrt::numeric(38, 17) AS dfrt, 
                        src:eqan::numeric(38, 17) AS eqan, 
                        src:eqty::integer AS eqty, 
                        src:eqty_kw::varchar AS eqty_kw, 
                        src:exep::integer AS exep, 
                        src:exep_kw::varchar AS exep_kw, 
                        src:exeq::integer AS exeq, 
                        src:exeq_kw::varchar AS exeq_kw, 
                        src:fidt::datetime AS fidt, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:prby::integer AS prby, 
                        src:prby_kw::varchar AS prby_kw, 
                        src:prdt::datetime AS prdt, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:qutm::numeric(38, 17) AS qutm, 
                        src:rtcc_1::numeric(38, 17) AS rtcc_1, 
                        src:rtcc_2::numeric(38, 17) AS rtcc_2, 
                        src:rtcc_3::numeric(38, 17) AS rtcc_3, 
                        src:rtfc_1::integer AS rtfc_1, 
                        src:rtfc_2::integer AS rtfc_2, 
                        src:rtfc_3::integer AS rtfc_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:shft::integer AS shft, 
                        src:timestamp::datetime AS timestamp, 
                        src:tsrl::integer AS tsrl, 
                        src:tsrl_kw::varchar AS tsrl_kw, 
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
                src:sern,
                src:cequ,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:sern,
                src:cequ,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPTC147 as strm))