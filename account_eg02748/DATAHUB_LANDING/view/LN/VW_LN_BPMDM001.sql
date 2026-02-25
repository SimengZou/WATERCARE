CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_BPMDM001 AS SELECT
                        src:bano::varchar AS bano, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cedt::date AS cedt, 
                        src:cist::varchar AS cist, 
                        src:compnr::integer AS compnr, 
                        src:ctrg::varchar AS ctrg, 
                        src:daob::date AS daob, 
                        src:deleted::boolean AS deleted, 
                        src:edte::date AS edte, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:emtp::integer AS emtp, 
                        src:emtp_kw::varchar AS emtp_kw, 
                        src:finr::varchar AS finr, 
                        src:hwem::numeric(38, 17) AS hwem, 
                        src:jobt::object AS jobt, 
                        src:mail::object AS mail, 
                        src:msad::object AS msad, 
                        src:msty::integer AS msty, 
                        src:msty_kw::varchar AS msty_kw, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:prtn::object AS prtn, 
                        src:ptbp::varchar AS ptbp, 
                        src:ptbp_ref_compnr::integer AS ptbp_ref_compnr, 
                        src:rgno::object AS rgno, 
                        src:sdte::date AS sdte, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sexe::integer AS sexe, 
                        src:sexe_kw::varchar AS sexe_kw, 
                        src:supv::varchar AS supv, 
                        src:supv_ref_compnr::integer AS supv_ref_compnr, 
                        src:tctr::object AS tctr, 
                        src:tefw::varchar AS tefw, 
                        src:telm::varchar AS telm, 
                        src:telw::varchar AS telw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlw1::varchar AS tlw1, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:ucrm::integer AS ucrm, 
                        src:ucrm_kw::varchar AS ucrm_kw, 
                        src:username::varchar AS username, 
                        src:wtsc::varchar AS wtsc, 
                        src:wtsc_ref_compnr::integer AS wtsc_ref_compnr, 
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
    
                        
                src:emno,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:emno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_BPMDM001 as strm))