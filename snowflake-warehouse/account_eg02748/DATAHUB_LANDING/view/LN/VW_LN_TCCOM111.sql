CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCOM111 AS SELECT
                        src:aetc::integer AS aetc, 
                        src:aetc_kw::varchar AS aetc_kw, 
                        src:apsr::integer AS apsr, 
                        src:apsr_kw::varchar AS apsr_kw, 
                        src:bpst::integer AS bpst, 
                        src:bpst_kw::varchar AS bpst_kw, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:capp::integer AS capp, 
                        src:capp_kw::varchar AS capp_kw, 
                        src:cbps::varchar AS cbps, 
                        src:cbps_ref_compnr::integer AS cbps_ref_compnr, 
                        src:cbtp::varchar AS cbtp, 
                        src:cbtp_ref_compnr::integer AS cbtp_ref_compnr, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:ccnt::varchar AS ccnt, 
                        src:ccnt_ref_compnr::integer AS ccnt_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:cfrw::varchar AS cfrw, 
                        src:cfrw_ref_compnr::integer AS cfrw_ref_compnr, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:endt::datetime AS endt, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:mask::varchar AS mask, 
                        src:mask_ref_compnr::integer AS mask_ref_compnr, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:rdec::varchar AS rdec, 
                        src:rdec_ref_compnr::integer AS rdec_ref_compnr, 
                        src:rfia::integer AS rfia, 
                        src:rfia_kw::varchar AS rfia_kw, 
                        src:rfis::varchar AS rfis, 
                        src:rrqt::integer AS rrqt, 
                        src:rrqt_kw::varchar AS rrqt_kw, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serv::varchar AS serv, 
                        src:serv_ref_compnr::integer AS serv_ref_compnr, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:sscc::integer AS sscc, 
                        src:sscc_kw::varchar AS sscc_kw, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:stdt::datetime AS stdt, 
                        src:tefx::varchar AS tefx, 
                        src:telp::varchar AS telp, 
                        src:tfci::varchar AS tfci, 
                        src:tfcy::varchar AS tfcy, 
                        src:tfex::varchar AS tfex, 
                        src:tfln::varchar AS tfln, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlci::varchar AS tlci, 
                        src:tlcy::varchar AS tlcy, 
                        src:tlex::varchar AS tlex, 
                        src:tlln::varchar AS tlln, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:user::varchar AS user, 
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
    
                        
                src:stbp,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:stbp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCOM111 as strm))