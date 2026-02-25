CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS048 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:cpcp::varchar AS cpcp, 
                        src:cpus::integer AS cpus, 
                        src:cpus_kw::varchar AS cpus_kw, 
                        src:cref::integer AS cref, 
                        src:cref_kw::varchar AS cref_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dinc::integer AS dinc, 
                        src:dinc_kw::varchar AS dinc_kw, 
                        src:dsca::object AS dsca, 
                        src:fxvc::integer AS fxvc, 
                        src:fxvc_kw::varchar AS fxvc_kw, 
                        src:iinv::integer AS iinv, 
                        src:iinv_kw::varchar AS iinv_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpoc::integer AS tpoc, 
                        src:tpoc_kw::varchar AS tpoc_kw, 
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
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cpcp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS048 as strm))