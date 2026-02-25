CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDSMI110 AS SELECT
                        src:acdt::datetime AS acdt, 
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:catt::varchar AS catt, 
                        src:catt_ref_compnr::integer AS catt_ref_compnr, 
                        src:ccor::varchar AS ccor, 
                        src:ccor_ref_compnr::integer AS ccor_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdis::varchar AS cdis, 
                        src:cdis_ref_compnr::integer AS cdis_ref_compnr, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpha::varchar AS cpha, 
                        src:cpha_ref_compnr::integer AS cpha_ref_compnr, 
                        src:cptp::varchar AS cptp, 
                        src:cptp_ref_compnr::integer AS cptp_ref_compnr, 
                        src:crdt::datetime AS crdt, 
                        src:crep::varchar AS crep, 
                        src:crep_ref_compnr::integer AS crep_ref_compnr, 
                        src:csou::varchar AS csou, 
                        src:csou_ref_compnr::integer AS csou_ref_compnr, 
                        src:dcdt::datetime AS dcdt, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:fcdt::datetime AS fcdt, 
                        src:guid::varchar AS guid, 
                        src:incf::integer AS incf, 
                        src:incf_kw::varchar AS incf_kw, 
                        src:lmus::varchar AS lmus, 
                        src:ltdt::datetime AS ltdt, 
                        src:mprj::varchar AS mprj, 
                        src:name::object AS name, 
                        src:opst::integer AS opst, 
                        src:opst_kw::varchar AS opst_kw, 
                        src:opty::varchar AS opty, 
                        src:role::integer AS role, 
                        src:role_kw::varchar AS role_kw, 
                        src:sapr::varchar AS sapr, 
                        src:sapr_cpha_ref_compnr::integer AS sapr_cpha_ref_compnr, 
                        src:sapr_ref_compnr::integer AS sapr_ref_compnr, 
                        src:sccp::numeric(38, 17) AS sccp, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:seri::varchar AS seri, 
                        src:tefx::varchar AS tefx, 
                        src:telp::varchar AS telp, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:xpam::numeric(38, 17) AS xpam, 
                        src:xpam_dtwc::numeric(38, 17) AS xpam_dtwc, 
                        src:xpam_lclc::numeric(38, 17) AS xpam_lclc, 
                        src:xpam_rfrc::numeric(38, 17) AS xpam_rfrc, 
                        src:xpam_rpc1::numeric(38, 17) AS xpam_rpc1, 
                        src:xpam_rpc2::numeric(38, 17) AS xpam_rpc2, 
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
                src:opty,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:opty  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDSMI110 as strm))