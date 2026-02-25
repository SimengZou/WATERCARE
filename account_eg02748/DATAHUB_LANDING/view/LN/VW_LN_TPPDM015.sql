CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPDM015 AS SELECT
                        src:acta::integer AS acta, 
                        src:acta_kw::varchar AS acta_kw, 
                        src:actp::integer AS actp, 
                        src:actp_kw::varchar AS actp_kw, 
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:ccco::varchar AS ccco, 
                        src:ccco_ref_compnr::integer AS ccco_ref_compnr, 
                        src:cccr::varchar AS cccr, 
                        src:cccr_ref_compnr::integer AS cccr_ref_compnr, 
                        src:ccfu::integer AS ccfu, 
                        src:ccfu_kw::varchar AS ccfu_kw, 
                        src:ccir::varchar AS ccir, 
                        src:ccir_ref_compnr::integer AS ccir_ref_compnr, 
                        src:ccsr::varchar AS ccsr, 
                        src:ccsr_ref_compnr::integer AS ccsr_ref_compnr, 
                        src:ccts::varchar AS ccts, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:ctrg::varchar AS ctrg, 
                        src:ctrg_ref_compnr::integer AS ctrg_ref_compnr, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:eplf::numeric(38, 17) AS eplf, 
                        src:ltrc::datetime AS ltrc, 
                        src:ltri::datetime AS ltri, 
                        src:ltrs::datetime AS ltrs, 
                        src:norm::numeric(38, 17) AS norm, 
                        src:prre::integer AS prre, 
                        src:prre_kw::varchar AS prre_kw, 
                        src:ratc::numeric(38, 17) AS ratc, 
                        src:rati::numeric(38, 17) AS rati, 
                        src:rats::numeric(38, 17) AS rats, 
                        src:rcmc_1::numeric(38, 17) AS rcmc_1, 
                        src:rcmc_2::numeric(38, 17) AS rcmc_2, 
                        src:rcmc_3::numeric(38, 17) AS rcmc_3, 
                        src:rfac::varchar AS rfac, 
                        src:rimc_1::numeric(38, 17) AS rimc_1, 
                        src:rimc_2::numeric(38, 17) AS rimc_2, 
                        src:rimc_3::numeric(38, 17) AS rimc_3, 
                        src:rsmc_1::numeric(38, 17) AS rsmc_1, 
                        src:rsmc_2::numeric(38, 17) AS rsmc_2, 
                        src:rsmc_3::numeric(38, 17) AS rsmc_3, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:task::varchar AS task, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:unrt::varchar AS unrt, 
                        src:unrt_ref_compnr::integer AS unrt_ref_compnr, 
                        src:username::varchar AS username, 
                        src:usyn::integer AS usyn, 
                        src:usyn_kw::varchar AS usyn_kw, 
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
    
                        
                src:task,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:task,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPDM015 as strm))