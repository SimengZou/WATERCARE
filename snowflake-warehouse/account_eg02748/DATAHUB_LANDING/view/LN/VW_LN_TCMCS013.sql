CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS013 AS SELECT
                        src:atie::integer AS atie, 
                        src:atie_kw::varchar AS atie_kw, 
                        src:cdde::integer AS cdde, 
                        src:cdde_kw::varchar AS cdde_kw, 
                        src:cdis::integer AS cdis, 
                        src:cdis_kw::varchar AS cdis_kw, 
                        src:compnr::integer AS compnr, 
                        src:cpay::varchar AS cpay, 
                        src:day1::integer AS day1, 
                        src:day2::integer AS day2, 
                        src:day3::integer AS day3, 
                        src:deleted::boolean AS deleted, 
                        src:disa::integer AS disa, 
                        src:disb::integer AS disb, 
                        src:disc::integer AS disc, 
                        src:dsca::object AS dsca, 
                        src:dtbs::integer AS dtbs, 
                        src:dtbs_kw::varchar AS dtbs_kw, 
                        src:fdis::integer AS fdis, 
                        src:fdue::integer AS fdue, 
                        src:pash::varchar AS pash, 
                        src:pash_ref_compnr::integer AS pash_ref_compnr, 
                        src:pdin::integer AS pdin, 
                        src:pdin_kw::varchar AS pdin_kw, 
                        src:pdis::integer AS pdis, 
                        src:pdis_kw::varchar AS pdis_kw, 
                        src:pdyn::integer AS pdyn, 
                        src:pdyn_kw::varchar AS pdyn_kw, 
                        src:pper::integer AS pper, 
                        src:prca::numeric(38, 17) AS prca, 
                        src:prcb::numeric(38, 17) AS prcb, 
                        src:prcc::numeric(38, 17) AS prcc, 
                        src:prio::integer AS prio, 
                        src:prio_kw::varchar AS prio_kw, 
                        src:ptyp::integer AS ptyp, 
                        src:ptyp_kw::varchar AS ptyp_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlsd::integer AS tlsd, 
                        src:tola::numeric(38, 17) AS tola, 
                        src:told::numeric(38, 17) AS told, 
                        src:tolp::integer AS tolp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:vtpm::varchar AS vtpm, 
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
                src:cpay,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cpay  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS013 as strm))