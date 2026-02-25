CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC160 AS SELECT
                        src:appr::integer AS appr, 
                        src:appr_kw::varchar AS appr_kw, 
                        src:cact::varchar AS cact, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:date::datetime AS date, 
                        src:deleted::boolean AS deleted, 
                        src:prco::integer AS prco, 
                        src:prco_kw::varchar AS prco_kw, 
                        src:prst::numeric(38, 17) AS prst, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:txtn::integer AS txtn, 
                        src:txtn_ref_compnr::integer AS txtn_ref_compnr, 
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
                src:cact,
                src:date,
                src:cpla,
                src:cprj,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cact,
                src:date,
                src:cpla,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC160 as strm))