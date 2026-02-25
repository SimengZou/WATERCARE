CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPPC600 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:appl::integer AS appl, 
                        src:appl_kw::varchar AS appl_kw, 
                        src:bamt::numeric(38, 17) AS bamt, 
                        src:base::varchar AS base, 
                        src:base_vrsn_ref_compnr::integer AS base_vrsn_ref_compnr, 
                        src:bclc::numeric(38, 17) AS bclc, 
                        src:bcur::varchar AS bcur, 
                        src:bprc::numeric(38, 17) AS bprc, 
                        src:brte::numeric(38, 17) AS brte, 
                        src:btba::numeric(38, 17) AS btba, 
                        src:btyp::integer AS btyp, 
                        src:btyp_kw::varchar AS btyp_kw, 
                        src:cact::varchar AS cact, 
                        src:calc::numeric(38, 17) AS calc, 
                        src:cccp::varchar AS cccp, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cldt::datetime AS cldt, 
                        src:compnr::integer AS compnr, 
                        src:coob::varchar AS coob, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:cprj::varchar AS cprj, 
                        src:cspa::varchar AS cspa, 
                        src:ctdt::datetime AS ctdt, 
                        src:deleted::boolean AS deleted, 
                        src:hour::numeric(38, 17) AS hour, 
                        src:levl::integer AS levl, 
                        src:ohcs::integer AS ohcs, 
                        src:ohst::integer AS ohst, 
                        src:ohst_kw::varchar AS ohst_kw, 
                        src:perc::numeric(38, 17) AS perc, 
                        src:rate::numeric(38, 17) AS rate, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:tbap::numeric(38, 17) AS tbap, 
                        src:tcob::varchar AS tcob, 
                        src:tcob_ref_compnr::integer AS tcob_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:unit::varchar AS unit, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:vrsn::integer AS vrsn, 
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
    
                        
                src:cotp,
                src:compnr,
                src:user,
                src:coob,
                src:cprj,
                src:base,
                src:cspa,
                src:ohcs,
                src:cact,
                src:sern,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cotp,
                src:compnr,
                src:user,
                src:coob,
                src:cprj,
                src:base,
                src:cspa,
                src:ohcs,
                src:cact,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPPC600 as strm))