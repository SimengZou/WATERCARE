CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPCTM060 AS SELECT
                        src:cact::varchar AS cact, 
                        src:cnln::varchar AS cnln, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:cono_ref_compnr::integer AS cono_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cspa_ref_compnr::integer AS cprj_cspa_ref_compnr, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:deleted::boolean AS deleted, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
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
    
                        
                src:sern,
                src:cono,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:sern,
                src:cono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPCTM060 as strm))