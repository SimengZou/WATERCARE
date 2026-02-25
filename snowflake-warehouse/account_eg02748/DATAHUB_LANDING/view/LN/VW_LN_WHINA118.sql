CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINA118 AS SELECT
                        src:amnt_1::numeric(38, 17) AS amnt_1, 
                        src:amnt_2::numeric(38, 17) AS amnt_2, 
                        src:amnt_3::numeric(38, 17) AS amnt_3, 
                        src:appb::varchar AS appb, 
                        src:appr::integer AS appr, 
                        src:appr_kw::varchar AS appr_kw, 
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:koor::integer AS koor, 
                        src:koor_kw::varchar AS koor_kw, 
                        src:lgdt::datetime AS lgdt, 
                        src:orno::varchar AS orno, 
                        src:pono::integer AS pono, 
                        src:pyps::integer AS pyps, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:rcln::integer AS rcln, 
                        src:rcno::varchar AS rcno, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:srnb::integer AS srnb, 
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
    
                        
                src:item,
                src:cwar,
                src:compnr,
                src:trdt,
                src:seqn,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:item,
                src:cwar,
                src:compnr,
                src:trdt,
                src:seqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINA118 as strm))