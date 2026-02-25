CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFGLD204 AS SELECT
                        src:bpid::varchar AS bpid, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::integer AS cono, 
                        src:deleted::boolean AS deleted, 
                        src:dimx::varchar AS dimx, 
                        src:dtyp::integer AS dtyp, 
                        src:duac::integer AS duac, 
                        src:duac_kw::varchar AS duac_kw, 
                        src:fobh_1::numeric(38, 17) AS fobh_1, 
                        src:fobh_2::numeric(38, 17) AS fobh_2, 
                        src:fobh_3::numeric(38, 17) AS fobh_3, 
                        src:ftob::numeric(38, 17) AS ftob, 
                        src:leac::varchar AS leac, 
                        src:nob1::numeric(38, 17) AS nob1, 
                        src:nob2::numeric(38, 17) AS nob2, 
                        src:nobh_1::numeric(38, 17) AS nobh_1, 
                        src:nobh_2::numeric(38, 17) AS nobh_2, 
                        src:nobh_3::numeric(38, 17) AS nobh_3, 
                        src:ntob::numeric(38, 17) AS ntob, 
                        src:qob1::numeric(38, 17) AS qob1, 
                        src:qob2::numeric(38, 17) AS qob2, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, 
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
                src:dtyp,
                src:dimx,
                src:leac,
                src:ccur,
                src:cono,
                src:bpid,
                src:year,
                src:duac,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:dtyp,
                src:dimx,
                src:leac,
                src:ccur,
                src:cono,
                src:bpid,
                src:year,
                src:duac  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFGLD204 as strm))