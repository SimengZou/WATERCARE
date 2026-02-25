CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCOM124 AS SELECT
                        src:aofc::varchar AS aofc, 
                        src:aofc_ref_compnr::integer AS aofc_ref_compnr, 
                        src:bank::varchar AS bank, 
                        src:bcbs::integer AS bcbs, 
                        src:bcbs_kw::varchar AS bcbs_kw, 
                        src:bpst::integer AS bpst, 
                        src:bpst_kw::varchar AS bpst_kw, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:cban::varchar AS cban, 
                        src:cbps::varchar AS cbps, 
                        src:cbps_ref_compnr::integer AS cbps_ref_compnr, 
                        src:cbtp::varchar AS cbtp, 
                        src:cbtp_ref_compnr::integer AS cbtp_ref_compnr, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:ccnt::varchar AS ccnt, 
                        src:ccnt_ref_compnr::integer AS ccnt_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:cnsm::integer AS cnsm, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:deleted::boolean AS deleted, 
                        src:eded::integer AS eded, 
                        src:endt::datetime AS endt, 
                        src:exor::integer AS exor, 
                        src:exor_kw::varchar AS exor_kw, 
                        src:expo::integer AS expo, 
                        src:expo_kw::varchar AS expo_kw, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:mxtn::integer AS mxtn, 
                        src:ptbp::varchar AS ptbp, 
                        src:ptbp_cban_ref_compnr::integer AS ptbp_cban_ref_compnr, 
                        src:ptbp_ref_compnr::integer AS ptbp_ref_compnr, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stdt::datetime AS stdt, 
                        src:stxa::integer AS stxa, 
                        src:stxa_kw::varchar AS stxa_kw, 
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
                        src:tndm::integer AS tndm, 
                        src:tndm_kw::varchar AS tndm_kw, 
                        src:txno::object AS txno, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:uptc::integer AS uptc, 
                        src:uptc_kw::varchar AS uptc_kw, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:uwht::integer AS uwht, 
                        src:uwht_kw::varchar AS uwht_kw, 
                        src:vacc::varchar AS vacc, 
                        src:valf::datetime AS valf, 
                        src:valt::datetime AS valt, 
                        src:vryn::integer AS vryn, 
                        src:vryn_kw::varchar AS vryn_kw, 
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
                src:ptbp,
                src:cofc,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:ptbp,
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCOM124 as strm))