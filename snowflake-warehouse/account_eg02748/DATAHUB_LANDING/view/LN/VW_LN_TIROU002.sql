CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TIROU002 AS SELECT
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpcp::varchar AS cpcp, 
                        src:cpcp_ref_compnr::integer AS cpcp_ref_compnr, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_ref_compnr::integer AS cwoc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dque::numeric(38, 17) AS dque, 
                        src:dsca::object AS dsca, 
                        src:mccp::numeric(38, 17) AS mccp, 
                        src:mcno::varchar AS mcno, 
                        src:mcrt_1::numeric(38, 17) AS mcrt_1, 
                        src:mcrt_2::numeric(38, 17) AS mcrt_2, 
                        src:mcrt_3::numeric(38, 17) AS mcrt_3, 
                        src:mdcp::numeric(38, 17) AS mdcp, 
                        src:mere::integer AS mere, 
                        src:mere_kw::varchar AS mere_kw, 
                        src:mewo::varchar AS mewo, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
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
    
                        
                src:compnr,
                src:mcno,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:mcno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TIROU002 as strm))