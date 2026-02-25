CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFACP260 AS SELECT
                        src:byer::varchar AS byer, 
                        src:clus::varchar AS clus, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:idoc::integer AS idoc, 
                        src:ityp::varchar AS ityp, 
                        src:load::varchar AS load, 
                        src:loco::integer AS loco, 
                        src:logd::date AS logd, 
                        src:logt::integer AS logt, 
                        src:mess::object AS mess, 
                        src:orno::varchar AS orno, 
                        src:otyp::integer AS otyp, 
                        src:otyp_kw::varchar AS otyp_kw, 
                        src:pono::integer AS pono, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shpi::varchar AS shpi, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:user::varchar AS user, 
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
    
                        
                src:ityp,
                src:idoc,
                src:orno,
                src:loco,
                src:compnr,
                src:pono,
                src:sqnb,
                src:otyp,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ityp,
                src:idoc,
                src:orno,
                src:loco,
                src:compnr,
                src:pono,
                src:sqnb,
                src:otyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFACP260 as strm))