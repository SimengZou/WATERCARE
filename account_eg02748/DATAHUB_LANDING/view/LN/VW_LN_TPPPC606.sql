CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC606 AS SELECT
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
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_fcmp::integer AS cprj_fcmp, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:deleted::boolean AS deleted, 
                        src:eatc_cntc::numeric(38, 17) AS eatc_cntc, 
                        src:eatc_dwhc::numeric(38, 17) AS eatc_dwhc, 
                        src:eatc_homc::numeric(38, 17) AS eatc_homc, 
                        src:eatc_prjc::numeric(38, 17) AS eatc_prjc, 
                        src:eatc_refc::numeric(38, 17) AS eatc_refc, 
                        src:eatc_rpc1::numeric(38, 17) AS eatc_rpc1, 
                        src:eatc_rpc2::numeric(38, 17) AS eatc_rpc2, 
                        src:eatc_trnc::numeric(38, 17) AS eatc_trnc, 
                        src:etoc_cntc::numeric(38, 17) AS etoc_cntc, 
                        src:etoc_dwhc::numeric(38, 17) AS etoc_dwhc, 
                        src:etoc_homc::numeric(38, 17) AS etoc_homc, 
                        src:etoc_prjc::numeric(38, 17) AS etoc_prjc, 
                        src:etoc_refc::numeric(38, 17) AS etoc_refc, 
                        src:etoc_rpc1::numeric(38, 17) AS etoc_rpc1, 
                        src:etoc_rpc2::numeric(38, 17) AS etoc_rpc2, 
                        src:etoc_trnc::numeric(38, 17) AS etoc_trnc, 
                        src:ovhd::varchar AS ovhd, 
                        src:ovhd_ref_compnr::integer AS ovhd_ref_compnr, 
                        src:rdat::datetime AS rdat, 
                        src:rgdt::datetime AS rgdt, 
                        src:rtcc_1::numeric(38, 17) AS rtcc_1, 
                        src:rtcc_2::numeric(38, 17) AS rtcc_2, 
                        src:rtcc_3::numeric(38, 17) AS rtcc_3, 
                        src:rtfc_1::integer AS rtfc_1, 
                        src:rtfc_2::integer AS rtfc_2, 
                        src:rtfc_3::integer AS rtfc_3, 
                        src:sequencenumber::integer AS sequencenumber, 
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
    
                        
                src:cact,
                src:cprj,
                src:ovhd,
                src:cspa,
                src:rgdt,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cact,
                src:cprj,
                src:ovhd,
                src:cspa,
                src:rgdt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC606 as strm))