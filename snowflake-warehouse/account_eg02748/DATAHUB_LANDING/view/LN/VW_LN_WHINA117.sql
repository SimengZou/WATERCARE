CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINA117 AS SELECT
                        src:amnt_1::numeric(38, 17) AS amnt_1, 
                        src:amnt_2::numeric(38, 17) AS amnt_2, 
                        src:amnt_3::numeric(38, 17) AS amnt_3, 
                        src:compnr::integer AS compnr, 
                        src:cpcp::varchar AS cpcp, 
                        src:cpcp_ref_compnr::integer AS cpcp_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:hour::numeric(38, 17) AS hour, 
                        src:item::varchar AS item, 
                        src:item_cwar_trdt_seqn_ref_compnr::integer AS item_cwar_trdt_seqn_ref_compnr, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:namt_1::numeric(38, 17) AS namt_1, 
                        src:namt_2::numeric(38, 17) AS namt_2, 
                        src:namt_3::numeric(38, 17) AS namt_3, 
                        src:oamt_1::numeric(38, 17) AS oamt_1, 
                        src:oamt_2::numeric(38, 17) AS oamt_2, 
                        src:oamt_3::numeric(38, 17) AS oamt_3, 
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
    
                        
                src:cpcp,
                src:item,
                src:compnr,
                src:seqn,
                src:cwar,
                src:trdt,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cpcp,
                src:item,
                src:compnr,
                src:seqn,
                src:cwar,
                src:trdt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINA117 as strm))