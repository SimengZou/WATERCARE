CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDPUR200 AS SELECT
                        src:adep::varchar AS adep, 
                        src:adep_ref_compnr::integer AS adep_ref_compnr, 
                        src:adin::varchar AS adin, 
                        src:aemn::varchar AS aemn, 
                        src:aemn_ref_compnr::integer AS aemn_ref_compnr, 
                        src:cact::varchar AS cact, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:ccon::varchar AS ccon, 
                        src:ccon_ref_compnr::integer AS ccon_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cnty::integer AS cnty, 
                        src:cnty_kw::varchar AS cnty_kw, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:conv::integer AS conv, 
                        src:conv_kw::varchar AS conv_kw, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cstl::varchar AS cstl, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:dadr::varchar AS dadr, 
                        src:dadr_ref_compnr::integer AS dadr_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dldt::datetime AS dldt, 
                        src:logn::varchar AS logn, 
                        src:ltdt::datetime AS ltdt, 
                        src:orig::integer AS orig, 
                        src:orig_kw::varchar AS orig_kw, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rdat::datetime AS rdat, 
                        src:rdep::varchar AS rdep, 
                        src:rdep_ref_compnr::integer AS rdep_ref_compnr, 
                        src:refa::object AS refa, 
                        src:refb::object AS refb, 
                        src:remn::varchar AS remn, 
                        src:remn_ref_compnr::integer AS remn_ref_compnr, 
                        src:rqno::varchar AS rqno, 
                        src:rqst::integer AS rqst, 
                        src:rqst_kw::varchar AS rqst_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:spap::integer AS spap, 
                        src:spap_kw::varchar AS spap_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:urgt::integer AS urgt, 
                        src:urgt_kw::varchar AS urgt_kw, 
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
                src:rqno,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:rqno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDPUR200 as strm))