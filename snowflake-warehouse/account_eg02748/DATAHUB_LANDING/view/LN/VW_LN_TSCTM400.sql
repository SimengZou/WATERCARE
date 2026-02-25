CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM400 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amnt_dwhc::numeric(38, 17) AS amnt_dwhc, 
                        src:amnt_homc::numeric(38, 17) AS amnt_homc, 
                        src:amnt_refc::numeric(38, 17) AS amnt_refc, 
                        src:amnt_rpc1::numeric(38, 17) AS amnt_rpc1, 
                        src:amnt_rpc2::numeric(38, 17) AS amnt_rpc2, 
                        src:cchn::integer AS cchn, 
                        src:cfln::integer AS cfln, 
                        src:compnr::integer AS compnr, 
                        src:csco::varchar AS csco, 
                        src:csco_cchn_ref_compnr::integer AS csco_cchn_ref_compnr, 
                        src:csco_ref_compnr::integer AS csco_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dimt::numeric(38, 17) AS dimt, 
                        src:dimt_dwhc::numeric(38, 17) AS dimt_dwhc, 
                        src:dimt_homc::numeric(38, 17) AS dimt_homc, 
                        src:dimt_refc::numeric(38, 17) AS dimt_refc, 
                        src:dimt_rpc1::numeric(38, 17) AS dimt_rpc1, 
                        src:dimt_rpc2::numeric(38, 17) AS dimt_rpc2, 
                        src:disc::numeric(38, 17) AS disc, 
                        src:efdt::datetime AS efdt, 
                        src:exdt::datetime AS exdt, 
                        src:fcmt_1::numeric(38, 17) AS fcmt_1, 
                        src:fcmt_2::numeric(38, 17) AS fcmt_2, 
                        src:fcmt_3::numeric(38, 17) AS fcmt_3, 
                        src:fcpc::numeric(38, 17) AS fcpc, 
                        src:fper::integer AS fper, 
                        src:fyer::integer AS fyer, 
                        src:icmp::integer AS icmp, 
                        src:icmt::numeric(38, 17) AS icmt, 
                        src:idoc::integer AS idoc, 
                        src:indt::datetime AS indt, 
                        src:inmt::numeric(38, 17) AS inmt, 
                        src:inst::integer AS inst, 
                        src:isst::integer AS isst, 
                        src:isst_kw::varchar AS isst_kw, 
                        src:istd::date AS istd, 
                        src:ityp::varchar AS ityp, 
                        src:nrpf::integer AS nrpf, 
                        src:ntmt::numeric(38, 17) AS ntmt, 
                        src:pidt::datetime AS pidt, 
                        src:prdt::datetime AS prdt, 
                        src:rahc_1::numeric(38, 17) AS rahc_1, 
                        src:rahc_2::numeric(38, 17) AS rahc_2, 
                        src:rahc_3::numeric(38, 17) AS rahc_3, 
                        src:rahc_cntc::numeric(38, 17) AS rahc_cntc, 
                        src:rahc_dwhc::numeric(38, 17) AS rahc_dwhc, 
                        src:rahc_refc::numeric(38, 17) AS rahc_refc, 
                        src:ratc_1::numeric(38, 17) AS ratc_1, 
                        src:ratc_2::numeric(38, 17) AS ratc_2, 
                        src:ratc_3::numeric(38, 17) AS ratc_3, 
                        src:ratd::datetime AS ratd, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stin::integer AS stin, 
                        src:stin_kw::varchar AS stin_kw, 
                        src:term::integer AS term, 
                        src:term_cfln_ref_compnr::integer AS term_cfln_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:txmt::numeric(38, 17) AS txmt, 
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
    
                        
                src:inst,
                src:compnr,
                src:csco,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:inst,
                src:compnr,
                src:csco  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM400 as strm))