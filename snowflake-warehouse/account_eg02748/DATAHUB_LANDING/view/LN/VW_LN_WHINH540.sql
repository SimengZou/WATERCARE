CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINH540 AS SELECT
                        src:clot::varchar AS clot, 
                        src:cntn::integer AS cntn, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_loca_ref_compnr::integer AS cwar_loca_ref_compnr, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:date::datetime AS date, 
                        src:deleted::boolean AS deleted, 
                        src:fcnt::integer AS fcnt, 
                        src:fcnt_kw::varchar AS fcnt_kw, 
                        src:item::varchar AS item, 
                        src:item_clot_ref_compnr::integer AS item_clot_ref_compnr, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:lcdt::datetime AS lcdt, 
                        src:loca::varchar AS loca, 
                        src:orno::varchar AS orno, 
                        src:orno_cntn_ref_compnr::integer AS orno_cntn_ref_compnr, 
                        src:pono::integer AS pono, 
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
    
                        
                src:clot,
                src:compnr,
                src:loca,
                src:date,
                src:cwar,
                src:item,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:clot,
                src:compnr,
                src:loca,
                src:date,
                src:cwar,
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINH540 as strm))