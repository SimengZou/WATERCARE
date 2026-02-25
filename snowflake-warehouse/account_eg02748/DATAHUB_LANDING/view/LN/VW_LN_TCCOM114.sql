CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCOM114 AS SELECT
                        src:aofc::varchar AS aofc, 
                        src:aofc_ref_compnr::integer AS aofc_ref_compnr, 
                        src:bank::varchar AS bank, 
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
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:deleted::boolean AS deleted, 
                        src:eded::integer AS eded, 
                        src:endt::datetime AS endt, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmus::varchar AS lmus, 
                        src:mrem::varchar AS mrem, 
                        src:mxtn::integer AS mxtn, 
                        src:pfbp::varchar AS pfbp, 
                        src:pfbp_cban_ref_compnr::integer AS pfbp_cban_ref_compnr, 
                        src:pfbp_ref_compnr::integer AS pfbp_ref_compnr, 
                        src:ppsl::integer AS ppsl, 
                        src:ppsl_kw::varchar AS ppsl_kw, 
                        src:prra::integer AS prra, 
                        src:prra_kw::varchar AS prra_kw, 
                        src:rtad::varchar AS rtad, 
                        src:rtad_ref_compnr::integer AS rtad_ref_compnr, 
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
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:vtcb::integer AS vtcb, 
                        src:vtcb_kw::varchar AS vtcb_kw, 
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
                src:cofc,
                src:pfbp,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cofc,
                src:pfbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCOM114 as strm))