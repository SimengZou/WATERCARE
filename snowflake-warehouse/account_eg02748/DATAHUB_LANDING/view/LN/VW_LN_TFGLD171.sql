CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFGLD171 AS SELECT
                        src:acid::varchar AS acid, 
                        src:acty::integer AS acty, 
                        src:acty_kw::varchar AS acty_kw, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:mapp::integer AS mapp, 
                        src:mapp_kw::varchar AS mapp_kw, 
                        src:paci::varchar AS paci, 
                        src:pseq::integer AS pseq, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:subl::integer AS subl, 
                        src:taxo::varchar AS taxo, 
                        src:taxo_vers_ref_compnr::integer AS taxo_vers_ref_compnr, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:unas::integer AS unas, 
                        src:unas_kw::varchar AS unas_kw, 
                        src:username::varchar AS username, 
                        src:vers::integer AS vers, 
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
                src:taxo,
                src:acid,
                src:vers,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:taxo,
                src:acid,
                src:vers  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFGLD171 as strm))