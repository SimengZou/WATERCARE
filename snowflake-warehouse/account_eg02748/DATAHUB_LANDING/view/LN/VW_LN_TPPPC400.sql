CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC400 AS SELECT
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
                        src:ambp_1::numeric(38, 17) AS ambp_1, 
                        src:ambp_2::numeric(38, 17) AS ambp_2, 
                        src:ambp_3::numeric(38, 17) AS ambp_3, 
                        src:ambp_4::numeric(38, 17) AS ambp_4, 
                        src:ambr_1::numeric(38, 17) AS ambr_1, 
                        src:ambr_2::numeric(38, 17) AS ambr_2, 
                        src:ambr_3::numeric(38, 17) AS ambr_3, 
                        src:ambr_4::numeric(38, 17) AS ambr_4, 
                        src:amdr_1::numeric(38, 17) AS amdr_1, 
                        src:amdr_2::numeric(38, 17) AS amdr_2, 
                        src:amdr_3::numeric(38, 17) AS amdr_3, 
                        src:amdr_4::numeric(38, 17) AS amdr_4, 
                        src:amds_1::numeric(38, 17) AS amds_1, 
                        src:amds_2::numeric(38, 17) AS amds_2, 
                        src:amds_3::numeric(38, 17) AS amds_3, 
                        src:amds_4::numeric(38, 17) AS amds_4, 
                        src:amep_1::numeric(38, 17) AS amep_1, 
                        src:amep_2::numeric(38, 17) AS amep_2, 
                        src:amep_3::numeric(38, 17) AS amep_3, 
                        src:amep_4::numeric(38, 17) AS amep_4, 
                        src:amex_1::numeric(38, 17) AS amex_1, 
                        src:amex_2::numeric(38, 17) AS amex_2, 
                        src:amex_3::numeric(38, 17) AS amex_3, 
                        src:amex_4::numeric(38, 17) AS amex_4, 
                        src:amfc_1::numeric(38, 17) AS amfc_1, 
                        src:amfc_2::numeric(38, 17) AS amfc_2, 
                        src:amfc_3::numeric(38, 17) AS amfc_3, 
                        src:amfc_4::numeric(38, 17) AS amfc_4, 
                        src:amfr_1::numeric(38, 17) AS amfr_1, 
                        src:amfr_2::numeric(38, 17) AS amfr_2, 
                        src:amfr_3::numeric(38, 17) AS amfr_3, 
                        src:amfr_4::numeric(38, 17) AS amfr_4, 
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
                        src:amrf_1::numeric(38, 17) AS amrf_1, 
                        src:amrf_2::numeric(38, 17) AS amrf_2, 
                        src:amrf_3::numeric(38, 17) AS amrf_3, 
                        src:amrf_4::numeric(38, 17) AS amrf_4, 
                        src:amrp_1::numeric(38, 17) AS amrp_1, 
                        src:amrp_2::numeric(38, 17) AS amrp_2, 
                        src:amrp_3::numeric(38, 17) AS amrp_3, 
                        src:amrp_4::numeric(38, 17) AS amrp_4, 
                        src:amrs_1::numeric(38, 17) AS amrs_1, 
                        src:amrs_2::numeric(38, 17) AS amrs_2, 
                        src:amrs_3::numeric(38, 17) AS amrs_3, 
                        src:amrs_4::numeric(38, 17) AS amrs_4, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:mprj::varchar AS mprj, 
                        src:mprj_ref_compnr::integer AS mprj_ref_compnr, 
                        src:mpto::integer AS mpto, 
                        src:mpto_kw::varchar AS mpto_kw, 
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
    
                        
                src:cprj,
                src:compnr,
                src:mpto,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:compnr,
                src:mpto  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC400 as strm))