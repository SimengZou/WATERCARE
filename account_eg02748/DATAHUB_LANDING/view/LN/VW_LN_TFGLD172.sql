CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFGLD172 AS SELECT
                        src:acid::varchar AS acid, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:fcom::integer AS fcom, 
                        src:fdim_1::varchar AS fdim_1, 
                        src:fdim_10::varchar AS fdim_10, 
                        src:fdim_11::varchar AS fdim_11, 
                        src:fdim_12::varchar AS fdim_12, 
                        src:fdim_2::varchar AS fdim_2, 
                        src:fdim_3::varchar AS fdim_3, 
                        src:fdim_4::varchar AS fdim_4, 
                        src:fdim_5::varchar AS fdim_5, 
                        src:fdim_6::varchar AS fdim_6, 
                        src:fdim_7::varchar AS fdim_7, 
                        src:fdim_8::varchar AS fdim_8, 
                        src:fdim_9::varchar AS fdim_9, 
                        src:flac::varchar AS flac, 
                        src:line::integer AS line, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:taxo::varchar AS taxo, 
                        src:taxo_vers_acid_ref_compnr::integer AS taxo_vers_acid_ref_compnr, 
                        src:tcom::integer AS tcom, 
                        src:tdim_1::varchar AS tdim_1, 
                        src:tdim_10::varchar AS tdim_10, 
                        src:tdim_11::varchar AS tdim_11, 
                        src:tdim_12::varchar AS tdim_12, 
                        src:tdim_2::varchar AS tdim_2, 
                        src:tdim_3::varchar AS tdim_3, 
                        src:tdim_4::varchar AS tdim_4, 
                        src:tdim_5::varchar AS tdim_5, 
                        src:tdim_6::varchar AS tdim_6, 
                        src:tdim_7::varchar AS tdim_7, 
                        src:tdim_8::varchar AS tdim_8, 
                        src:tdim_9::varchar AS tdim_9, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlac::varchar AS tlac, 
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
                src:line,
                src:taxo,
                src:acid,
                src:vers,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:line,
                src:taxo,
                src:acid,
                src:vers  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFGLD172 as strm))