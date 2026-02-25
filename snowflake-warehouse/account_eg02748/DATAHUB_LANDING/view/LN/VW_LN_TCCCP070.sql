CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCCP070 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:cpdt::varchar AS cpdt, 
                        src:cpdt_ref_compnr::integer AS cpdt_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:endt::datetime AS endt, 
                        src:peri::integer AS peri, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stdt::datetime AS stdt, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:yrno::integer AS yrno, 
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
    
                        
                src:yrno,
                src:compnr,
                src:cpdt,
                src:peri,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:yrno,
                src:compnr,
                src:cpdt,
                src:peri  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCCP070 as strm))