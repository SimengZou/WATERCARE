CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TIROU001 AS SELECT
                        src:bfem::varchar AS bfem, 
                        src:bfem_ref_compnr::integer AS bfem_ref_compnr, 
                        src:bfty::integer AS bfty, 
                        src:bfty_kw::varchar AS bfty_kw, 
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:ccap::integer AS ccap, 
                        src:ccap_kw::varchar AS ccap_kw, 
                        src:compnr::integer AS compnr, 
                        src:cpgr::varchar AS cpgr, 
                        src:crmp::integer AS crmp, 
                        src:crmp_kw::varchar AS crmp_kw, 
                        src:crtf::integer AS crtf, 
                        src:crty::integer AS crty, 
                        src:crty_kw::varchar AS crty_kw, 
                        src:ctwc::varchar AS ctwc, 
                        src:ctwc_ref_compnr::integer AS ctwc_ref_compnr, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_ref_compnr::integer AS cwoc_ref_compnr, 
                        src:dcru::numeric(38, 17) AS dcru, 
                        src:deleted::boolean AS deleted, 
                        src:dque::numeric(38, 17) AS dque, 
                        src:dsca::object AS dsca, 
                        src:kowc::integer AS kowc, 
                        src:kowc_kw::varchar AS kowc_kw, 
                        src:lbdv::varchar AS lbdv, 
                        src:mcgr::integer AS mcgr, 
                        src:mcgr_kw::varchar AS mcgr_kw, 
                        src:mnwc::varchar AS mnwc, 
                        src:mnwc_ref_compnr::integer AS mnwc_ref_compnr, 
                        src:mvtm::numeric(38, 17) AS mvtm, 
                        src:nomc::numeric(38, 17) AS nomc, 
                        src:noop::numeric(38, 17) AS noop, 
                        src:norp::integer AS norp, 
                        src:obfu::integer AS obfu, 
                        src:oprc::varchar AS oprc, 
                        src:oprc_ref_compnr::integer AS oprc_ref_compnr, 
                        src:pddp::varchar AS pddp, 
                        src:pddp_rcmp::integer AS pddp_rcmp, 
                        src:pddp_ref_compnr::integer AS pddp_ref_compnr, 
                        src:plgr::varchar AS plgr, 
                        src:prin::integer AS prin, 
                        src:prin_kw::varchar AS prin_kw, 
                        src:qutm::numeric(38, 17) AS qutm, 
                        src:rlbr::integer AS rlbr, 
                        src:rlbr_kw::varchar AS rlbr_kw, 
                        src:"rows"::integer AS "rows", 
                        src:rows_kw::varchar AS rows_kw, 
                        src:rprd::integer AS rprd, 
                        src:rprd_kw::varchar AS rprd_kw, 
                        src:rpwc::varchar AS rpwc, 
                        src:rpwc_ref_compnr::integer AS rpwc_ref_compnr, 
                        src:scid::integer AS scid, 
                        src:scid_kw::varchar AS scid_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:stty::integer AS stty, 
                        src:stty_kw::varchar AS stty_kw, 
                        src:swcc::integer AS swcc, 
                        src:swct::integer AS swct, 
                        src:swct_kw::varchar AS swct_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tuni::integer AS tuni, 
                        src:tuni_kw::varchar AS tuni_kw, 
                        src:upcs::integer AS upcs, 
                        src:upcs_kw::varchar AS upcs_kw, 
                        src:usco::integer AS usco, 
                        src:usco_kw::varchar AS usco_kw, 
                        src:username::varchar AS username, 
                        src:wcpg::varchar AS wcpg, 
                        src:wcpg_ref_compnr::integer AS wcpg_ref_compnr, 
                        src:wcru::numeric(38, 17) AS wcru, 
                        src:wipw::varchar AS wipw, 
                        src:wipw_ref_compnr::integer AS wipw_ref_compnr, 
                        src:wttm::numeric(38, 17) AS wttm, 
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
                src:cwoc,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cwoc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TIROU001 as strm))