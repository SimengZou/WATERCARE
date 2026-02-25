CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC205 AS SELECT
                        src:cact::varchar AS cact, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:deleted::boolean AS deleted, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:entu::varchar AS entu, 
                        src:frdt::datetime AS frdt, 
                        src:hper::integer AS hper, 
                        src:hyea::integer AS hyea, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:peri::integer AS peri, 
                        src:rgdt::datetime AS rgdt, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:user::varchar AS user, 
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
                src:cprj,
                src:user,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cprj,
                src:user  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC205 as strm))