CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSMDM105 AS SELECT
                        src:artp::integer AS artp, 
                        src:artp_kw::varchar AS artp_kw, 
                        src:ccha::numeric(38, 17) AS ccha, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:clrt::varchar AS clrt, 
                        src:clrt_ref_compnr::integer AS clrt_ref_compnr, 
                        src:cmsa::varchar AS cmsa, 
                        src:cmsa_ref_compnr::integer AS cmsa_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:csar::varchar AS csar, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_ref_compnr::integer AS cwoc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dist::numeric(38, 17) AS dist, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:trti::numeric(38, 17) AS trti, 
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
                src:csar,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:csar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSMDM105 as strm))