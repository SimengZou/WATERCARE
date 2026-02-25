CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINH435 AS SELECT
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:carp::varchar AS carp, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:crte::varchar AS crte, 
                        src:crte_ref_compnr::integer AS crte_ref_compnr, 
                        src:cwun::varchar AS cwun, 
                        src:cwun_ref_compnr::integer AS cwun_ref_compnr, 
                        src:dcdt::datetime AS dcdt, 
                        src:delc::varchar AS delc, 
                        src:delc_ref_compnr::integer AS delc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:deln::varchar AS deln, 
                        src:depc::integer AS depc, 
                        src:depc_ref_compnr::integer AS depc_ref_compnr, 
                        src:dnst::integer AS dnst, 
                        src:dnst_kw::varchar AS dnst_kw, 
                        src:finp::integer AS finp, 
                        src:finp_kw::varchar AS finp_kw, 
                        src:fnsr::varchar AS fnsr, 
                        src:fsyr::integer AS fsyr, 
                        src:grwt::numeric(38, 17) AS grwt, 
                        src:itbp::varchar AS itbp, 
                        src:itbp_ref_compnr::integer AS itbp_ref_compnr, 
                        src:load::varchar AS load, 
                        src:load_ref_compnr::integer AS load_ref_compnr, 
                        src:manl::integer AS manl, 
                        src:manl_kw::varchar AS manl_kw, 
                        src:motv::varchar AS motv, 
                        src:motv_ref_compnr::integer AS motv_ref_compnr, 
                        src:nota::object AS nota, 
                        src:notb::object AS notb, 
                        src:notc::object AS notc, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:prdn::varchar AS prdn, 
                        src:prdt::datetime AS prdt, 
                        src:qpac::numeric(38, 17) AS qpac, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfad::varchar AS sfad, 
                        src:sfad_ref_compnr::integer AS sfad_ref_compnr, 
                        src:sfco::varchar AS sfco, 
                        src:sfcp::integer AS sfcp, 
                        src:sfty::integer AS sfty, 
                        src:sfty_kw::varchar AS sfty_kw, 
                        src:shap::object AS shap, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:stad::varchar AS stad, 
                        src:stad_ref_compnr::integer AS stad_ref_compnr, 
                        src:stco::varchar AS stco, 
                        src:stcp::integer AS stcp, 
                        src:stty::integer AS stty, 
                        src:stty_kw::varchar AS stty_kw, 
                        src:taxp::varchar AS taxp, 
                        src:taxs::varchar AS taxs, 
                        src:timestamp::datetime AS timestamp, 
                        src:tsad::varchar AS tsad, 
                        src:tsad_ref_compnr::integer AS tsad_ref_compnr, 
                        src:username::varchar AS username, 
                        src:wdep::varchar AS wdep, 
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
                src:prdn,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:prdn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINH435 as strm))