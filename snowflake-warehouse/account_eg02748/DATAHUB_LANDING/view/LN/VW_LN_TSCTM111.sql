CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM111 AS SELECT
                        src:cact::varchar AS cact, 
                        src:cact_ref_compnr::integer AS cact_ref_compnr, 
                        src:camr::varchar AS camr, 
                        src:camr_ref_compnr::integer AS camr_ref_compnr, 
                        src:camt_cntc::numeric(38, 17) AS camt_cntc, 
                        src:camt_dwhc::numeric(38, 17) AS camt_dwhc, 
                        src:camt_homc::numeric(38, 17) AS camt_homc, 
                        src:camt_refc::numeric(38, 17) AS camt_refc, 
                        src:camt_rpc1::numeric(38, 17) AS camt_rpc1, 
                        src:camt_rpc2::numeric(38, 17) AS camt_rpc2, 
                        src:caro::varchar AS caro, 
                        src:caro_ref_compnr::integer AS caro_ref_compnr, 
                        src:cfln::integer AS cfln, 
                        src:compnr::integer AS compnr, 
                        src:cseq::integer AS cseq, 
                        src:deleted::boolean AS deleted, 
                        src:fplv::integer AS fplv, 
                        src:fplv_kw::varchar AS fplv_kw, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:pris_dwhc::numeric(38, 17) AS pris_dwhc, 
                        src:pris_homc::numeric(38, 17) AS pris_homc, 
                        src:pris_refc::numeric(38, 17) AS pris_refc, 
                        src:pris_rpc1::numeric(38, 17) AS pris_rpc1, 
                        src:pris_rpc2::numeric(38, 17) AS pris_rpc2, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:term::integer AS term, 
                        src:term_cfln_ref_compnr::integer AS term_cfln_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
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
    
                        
                src:cseq,
                src:compnr,
                src:term,
                src:cfln,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cseq,
                src:compnr,
                src:term,
                src:cfln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM111 as strm))