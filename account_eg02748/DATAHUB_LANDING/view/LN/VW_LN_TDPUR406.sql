CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDPUR406 AS SELECT
                        src:afrw::varchar AS afrw, 
                        src:afrw_ref_compnr::integer AS afrw_ref_compnr, 
                        src:amld::numeric(38, 17) AS amld, 
                        src:amod::numeric(38, 17) AS amod, 
                        src:arej::integer AS arej, 
                        src:arej_kw::varchar AS arej_kw, 
                        src:asnl::object AS asnl, 
                        src:asno::object AS asno, 
                        src:bgxc::integer AS bgxc, 
                        src:bgxc_kw::varchar AS bgxc_kw, 
                        src:cdf_idet::object AS cdf_idet, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:conf::integer AS conf, 
                        src:conf_kw::varchar AS conf_kw, 
                        src:coop_1::numeric(38, 17) AS coop_1, 
                        src:coop_2::numeric(38, 17) AS coop_2, 
                        src:coop_3::numeric(38, 17) AS coop_3, 
                        src:copr_1::numeric(38, 17) AS copr_1, 
                        src:copr_2::numeric(38, 17) AS copr_2, 
                        src:copr_3::numeric(38, 17) AS copr_3, 
                        src:crej::varchar AS crej, 
                        src:crej_ref_compnr::integer AS crej_ref_compnr, 
                        src:cuva::numeric(38, 17) AS cuva, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:damt::numeric(38, 17) AS damt, 
                        src:ddte::datetime AS ddte, 
                        src:deleted::boolean AS deleted, 
                        src:deln::varchar AS deln, 
                        src:dino::object AS dino, 
                        src:fire::integer AS fire, 
                        src:fire_kw::varchar AS fire_kw, 
                        src:ldat::datetime AS ldat, 
                        src:load::varchar AS load, 
                        src:lssn::varchar AS lssn, 
                        src:lssn_ref_compnr::integer AS lssn_ref_compnr, 
                        src:mpnr::varchar AS mpnr, 
                        src:mpnr_cmnf_ref_compnr::integer AS mpnr_cmnf_ref_compnr, 
                        src:onpr::numeric(38, 17) AS onpr, 
                        src:orno::varchar AS orno, 
                        src:orno_ref_compnr::integer AS orno_ref_compnr, 
                        src:pono::integer AS pono, 
                        src:qiap::numeric(38, 17) AS qiap, 
                        src:qidl::numeric(38, 17) AS qidl, 
                        src:qips::numeric(38, 17) AS qips, 
                        src:qirj::numeric(38, 17) AS qirj, 
                        src:qpap::numeric(38, 17) AS qpap, 
                        src:qpdl::numeric(38, 17) AS qpdl, 
                        src:qpps::numeric(38, 17) AS qpps, 
                        src:qprj::numeric(38, 17) AS qprj, 
                        src:qual::integer AS qual, 
                        src:qual_kw::varchar AS qual_kw, 
                        src:rcld::datetime AS rcld, 
                        src:rcno::varchar AS rcno, 
                        src:refa::object AS refa, 
                        src:revi::varchar AS revi, 
                        src:rseq::integer AS rseq, 
                        src:rsqn::integer AS rsqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shdt::datetime AS shdt, 
                        src:shpm::varchar AS shpm, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:wght::numeric(38, 17) AS wght, 
                        src:wtun::varchar AS wtun, 
                        src:wtun_ref_compnr::integer AS wtun_ref_compnr, 
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
    
                        
                src:orno,
                src:pono,
                src:rsqn,
                src:sqnb,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:orno,
                src:pono,
                src:rsqn,
                src:sqnb,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDPUR406 as strm))