CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS001 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:crnd::numeric(38, 17) AS crnd, 
                        src:cuni::varchar AS cuni, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:icun::varchar AS icun, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:tccu::integer AS tccu, 
                        src:tccu_kw::varchar AS tccu_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:uccc::integer AS uccc, 
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
    
                        
                src:cuni,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cuni,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS001 as strm))