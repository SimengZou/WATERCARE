CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS065 AS SELECT
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:casi::varchar AS casi, 
                        src:casi_ref_compnr::integer AS casi_ref_compnr, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:city::object AS city, 
                        src:clrt::varchar AS clrt, 
                        src:clrt_ref_compnr::integer AS clrt_ref_compnr, 
                        src:comp::integer AS comp, 
                        src:compnr::integer AS compnr, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_eunt::varchar AS cwoc_eunt, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:eunt_rcmp::integer AS eunt_rcmp, 
                        src:ract::varchar AS ract, 
                        src:ract_ref_compnr::integer AS ract_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:typd::integer AS typd, 
                        src:typd_kw::varchar AS typd_kw, 
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
    
                        
                src:cwoc,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cwoc,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS065 as strm))