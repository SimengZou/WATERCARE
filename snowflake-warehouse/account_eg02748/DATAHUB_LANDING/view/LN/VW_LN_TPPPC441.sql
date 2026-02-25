CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC441 AS SELECT
                        src:amac_1::numeric(38, 17) AS amac_1, 
                        src:amac_2::numeric(38, 17) AS amac_2, 
                        src:amac_3::numeric(38, 17) AS amac_3, 
                        src:amac_4::numeric(38, 17) AS amac_4, 
                        src:amap_1::numeric(38, 17) AS amap_1, 
                        src:amap_2::numeric(38, 17) AS amap_2, 
                        src:amap_3::numeric(38, 17) AS amap_3, 
                        src:amap_4::numeric(38, 17) AS amap_4, 
                        src:ambg_1::numeric(38, 17) AS ambg_1, 
                        src:ambg_2::numeric(38, 17) AS ambg_2, 
                        src:ambg_3::numeric(38, 17) AS ambg_3, 
                        src:ambg_4::numeric(38, 17) AS ambg_4, 
                        src:amep_1::numeric(38, 17) AS amep_1, 
                        src:amep_2::numeric(38, 17) AS amep_2, 
                        src:amep_3::numeric(38, 17) AS amep_3, 
                        src:amep_4::numeric(38, 17) AS amep_4, 
                        src:amex_1::numeric(38, 17) AS amex_1, 
                        src:amex_2::numeric(38, 17) AS amex_2, 
                        src:amex_3::numeric(38, 17) AS amex_3, 
                        src:amex_4::numeric(38, 17) AS amex_4, 
                        src:ampe_1::numeric(38, 17) AS ampe_1, 
                        src:ampe_2::numeric(38, 17) AS ampe_2, 
                        src:ampe_3::numeric(38, 17) AS ampe_3, 
                        src:ampe_4::numeric(38, 17) AS ampe_4, 
                        src:ampm_1::numeric(38, 17) AS ampm_1, 
                        src:ampm_2::numeric(38, 17) AS ampm_2, 
                        src:ampm_3::numeric(38, 17) AS ampm_3, 
                        src:ampm_4::numeric(38, 17) AS ampm_4, 
                        src:ampp_1::numeric(38, 17) AS ampp_1, 
                        src:ampp_2::numeric(38, 17) AS ampp_2, 
                        src:ampp_3::numeric(38, 17) AS ampp_3, 
                        src:ampp_4::numeric(38, 17) AS ampp_4, 
                        src:cact::varchar AS cact, 
                        src:compnr::integer AS compnr, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:mpto::integer AS mpto, 
                        src:mpto_kw::varchar AS mpto_kw, 
                        src:quac::numeric(38, 17) AS quac, 
                        src:quap::numeric(38, 17) AS quap, 
                        src:qubg::numeric(38, 17) AS qubg, 
                        src:quep::numeric(38, 17) AS quep, 
                        src:quex::numeric(38, 17) AS quex, 
                        src:qupe::numeric(38, 17) AS qupe, 
                        src:qupm::numeric(38, 17) AS qupm, 
                        src:qupp::numeric(38, 17) AS qupp, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
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
    
                        
                src:mpto,
                src:compnr,
                src:cprj,
                src:cotp,
                src:cpla,
                src:cact,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:mpto,
                src:compnr,
                src:cprj,
                src:cotp,
                src:cpla,
                src:cact  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC441 as strm))