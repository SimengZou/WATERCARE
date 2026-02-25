CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPTC316 AS SELECT
                        src:aeqc_1::numeric(38, 17) AS aeqc_1, 
                        src:aeqc_2::numeric(38, 17) AS aeqc_2, 
                        src:aeqc_3::numeric(38, 17) AS aeqc_3, 
                        src:aeqc_4::numeric(38, 17) AS aeqc_4, 
                        src:aicc_1::numeric(38, 17) AS aicc_1, 
                        src:aicc_2::numeric(38, 17) AS aicc_2, 
                        src:aicc_3::numeric(38, 17) AS aicc_3, 
                        src:aicc_4::numeric(38, 17) AS aicc_4, 
                        src:aicl_1::numeric(38, 17) AS aicl_1, 
                        src:aicl_2::numeric(38, 17) AS aicl_2, 
                        src:aicl_3::numeric(38, 17) AS aicl_3, 
                        src:aicl_4::numeric(38, 17) AS aicl_4, 
                        src:amac_1::numeric(38, 17) AS amac_1, 
                        src:amac_2::numeric(38, 17) AS amac_2, 
                        src:amac_3::numeric(38, 17) AS amac_3, 
                        src:amac_4::numeric(38, 17) AS amac_4, 
                        src:asbc_1::numeric(38, 17) AS asbc_1, 
                        src:asbc_2::numeric(38, 17) AS asbc_2, 
                        src:asbc_3::numeric(38, 17) AS asbc_3, 
                        src:asbc_4::numeric(38, 17) AS asbc_4, 
                        src:atac_1::numeric(38, 17) AS atac_1, 
                        src:atac_2::numeric(38, 17) AS atac_2, 
                        src:atac_3::numeric(38, 17) AS atac_3, 
                        src:atac_4::numeric(38, 17) AS atac_4, 
                        src:cact::varchar AS cact, 
                        src:ccal::varchar AS ccal, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ccal_ref_compnr::integer AS cprj_ccal_ref_compnr, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:qtah::numeric(38, 17) AS qtah, 
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
    
                        
                src:cpla,
                src:cprj,
                src:cact,
                src:compnr,
                src:ccco,
                src:ccal,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cpla,
                src:cprj,
                src:cact,
                src:compnr,
                src:ccco,
                src:ccal  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPTC316 as strm))