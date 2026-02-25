CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM320 AS SELECT
                        src:actn::integer AS actn, 
                        src:actn_kw::varchar AS actn_kw, 
                        src:amdy::numeric(38, 17) AS amdy, 
                        src:camt_1::numeric(38, 17) AS camt_1, 
                        src:camt_2::numeric(38, 17) AS camt_2, 
                        src:camt_3::numeric(38, 17) AS camt_3, 
                        src:camt_cntc::numeric(38, 17) AS camt_cntc, 
                        src:camt_dwhc::numeric(38, 17) AS camt_dwhc, 
                        src:camt_refc::numeric(38, 17) AS camt_refc, 
                        src:cchn::integer AS cchn, 
                        src:chdt::date AS chdt, 
                        src:cind::varchar AS cind, 
                        src:cind_ref_compnr::integer AS cind_ref_compnr, 
                        src:cody_1::numeric(38, 17) AS cody_1, 
                        src:cody_2::numeric(38, 17) AS cody_2, 
                        src:cody_3::numeric(38, 17) AS cody_3, 
                        src:compnr::integer AS compnr, 
                        src:crtm::datetime AS crtm, 
                        src:csco::varchar AS csco, 
                        src:csco_ref_compnr::integer AS csco_ref_compnr, 
                        src:csmt::numeric(38, 17) AS csmt, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:efdt::date AS efdt, 
                        src:erfa::numeric(38, 17) AS erfa, 
                        src:exdt::date AS exdt, 
                        src:icpn::numeric(38, 17) AS icpn, 
                        src:inpc::numeric(38, 17) AS inpc, 
                        src:nrpe::integer AS nrpe, 
                        src:ochn::integer AS ochn, 
                        src:peru::integer AS peru, 
                        src:peru_kw::varchar AS peru_kw, 
                        src:rsmt::numeric(38, 17) AS rsmt, 
                        src:rsmt_dwhc::numeric(38, 17) AS rsmt_dwhc, 
                        src:rsmt_homc::numeric(38, 17) AS rsmt_homc, 
                        src:rsmt_refc::numeric(38, 17) AS rsmt_refc, 
                        src:rsmt_rpc1::numeric(38, 17) AS rsmt_rpc1, 
                        src:rsmt_rpc2::numeric(38, 17) AS rsmt_rpc2, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:term::integer AS term, 
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
    
                        
                src:csco,
                src:compnr,
                src:cchn,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:csco,
                src:compnr,
                src:cchn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM320 as strm))