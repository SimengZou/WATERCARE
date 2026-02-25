CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINA113 AS SELECT
                        src:amac_1::numeric(38, 17) AS amac_1, 
                        src:amac_2::numeric(38, 17) AS amac_2, 
                        src:amac_3::numeric(38, 17) AS amac_3, 
                        src:amah::numeric(38, 17) AS amah, 
                        src:amnt_1::numeric(38, 17) AS amnt_1, 
                        src:amnt_2::numeric(38, 17) AS amnt_2, 
                        src:amnt_3::numeric(38, 17) AS amnt_3, 
                        src:atmc_1::numeric(38, 17) AS atmc_1, 
                        src:atmc_2::numeric(38, 17) AS atmc_2, 
                        src:atmc_3::numeric(38, 17) AS atmc_3, 
                        src:atmh::numeric(38, 17) AS atmh, 
                        src:compnr::integer AS compnr, 
                        src:cpcp::varchar AS cpcp, 
                        src:cpcp_ref_compnr::integer AS cpcp_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:hour::numeric(38, 17) AS hour, 
                        src:inwp::integer AS inwp, 
                        src:inwp_kw::varchar AS inwp_kw, 
                        src:item::varchar AS item, 
                        src:item_cwar_trdt_seqn_inwp_ref_compnr::integer AS item_cwar_trdt_seqn_inwp_ref_compnr, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:matc_1::numeric(38, 17) AS matc_1, 
                        src:matc_2::numeric(38, 17) AS matc_2, 
                        src:matc_3::numeric(38, 17) AS matc_3, 
                        src:math::numeric(38, 17) AS math, 
                        src:mauc_1::numeric(38, 17) AS mauc_1, 
                        src:mauc_2::numeric(38, 17) AS mauc_2, 
                        src:mauc_3::numeric(38, 17) AS mauc_3, 
                        src:mauh::numeric(38, 17) AS mauh, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
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
    
                        
                src:compnr,
                src:cpcp,
                src:cwar,
                src:trdt,
                src:inwp,
                src:item,
                src:seqn,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cpcp,
                src:cwar,
                src:trdt,
                src:inwp,
                src:item,
                src:seqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINA113 as strm))