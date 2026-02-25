CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDPUR201 AS SELECT
                        src:adin::varchar AS adin, 
                        src:bgxc::integer AS bgxc, 
                        src:bgxc_kw::varchar AS bgxc_kw, 
                        src:cact::varchar AS cact, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccon::varchar AS ccon, 
                        src:ccon_ref_compnr::integer AS ccon_ref_compnr, 
                        src:cdf_idet::varchar AS cdf_idet, 
                        src:citt::varchar AS citt, 
                        src:citt_ref_compnr::integer AS citt_ref_compnr, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:cnty::integer AS cnty, 
                        src:cnty_kw::varchar AS cnty_kw, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crit::object AS crit, 
                        src:crrf::integer AS crrf, 
                        src:crrf_kw::varchar AS crrf_kw, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:cupp::varchar AS cupp, 
                        src:cupp_ref_compnr::integer AS cupp_ref_compnr, 
                        src:cuqp::varchar AS cuqp, 
                        src:cuqp_ref_compnr::integer AS cuqp_ref_compnr, 
                        src:cvpp::numeric(38, 17) AS cvpp, 
                        src:cvqp::numeric(38, 17) AS cvqp, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dldt::datetime AS dldt, 
                        src:effn::integer AS effn, 
                        src:effn_ref_compnr::integer AS effn_ref_compnr, 
                        src:glco::varchar AS glco, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:leng::numeric(38, 17) AS leng, 
                        src:mitm::object AS mitm, 
                        src:mpnr::varchar AS mpnr, 
                        src:mpnr_cmnf_ref_compnr::integer AS mpnr_cmnf_ref_compnr, 
                        src:nids::object AS nids, 
                        src:nsds::object AS nsds, 
                        src:oamt::numeric(38, 17) AS oamt, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:pegd::integer AS pegd, 
                        src:pegd_kw::varchar AS pegd_kw, 
                        src:pono::integer AS pono, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rejc::integer AS rejc, 
                        src:rejc_kw::varchar AS rejc_kw, 
                        src:rqno::varchar AS rqno, 
                        src:rqno_ref_compnr::integer AS rqno_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:thic::numeric(38, 17) AS thic, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:urgt::integer AS urgt, 
                        src:urgt_kw::varchar AS urgt_kw, 
                        src:username::varchar AS username, 
                        src:widt::numeric(38, 17) AS widt, 
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
    
                        
                src:pono,
                src:rqno,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:pono,
                src:rqno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDPUR201 as strm))