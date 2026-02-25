CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM200 AS SELECT
                        src:aexm::varchar AS aexm, 
                        src:aexm_ref_compnr::integer AS aexm_ref_compnr, 
                        src:agrp::varchar AS agrp, 
                        src:agrp_ref_compnr::integer AS agrp_ref_compnr, 
                        src:amth::varchar AS amth, 
                        src:amth_ref_compnr::integer AS amth_ref_compnr, 
                        src:anbm::varchar AS anbm, 
                        src:anbm_ref_compnr::integer AS anbm_ref_compnr, 
                        src:aslf::integer AS aslf, 
                        src:bslv::integer AS bslv, 
                        src:bslv_kw::varchar AS bslv_kw, 
                        src:cmth::varchar AS cmth, 
                        src:cmth_ref_compnr::integer AS cmth_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:ctgy::varchar AS ctgy, 
                        src:cthc::varchar AS cthc, 
                        src:cthr::numeric(38, 17) AS cthr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dscr::integer AS dscr, 
                        src:dscr_kw::varchar AS dscr_kw, 
                        src:fadr::numeric(38, 17) AS fadr, 
                        src:fmth::varchar AS fmth, 
                        src:fmth_ref_compnr::integer AS fmth_ref_compnr, 
                        src:itcd::integer AS itcd, 
                        src:itcd_kw::varchar AS itcd_kw, 
                        src:life::integer AS life, 
                        src:lmth::varchar AS lmth, 
                        src:lmth_ref_compnr::integer AS lmth_ref_compnr, 
                        src:mmth::varchar AS mmth, 
                        src:mmth_ref_compnr::integer AS mmth_ref_compnr, 
                        src:mskc::integer AS mskc, 
                        src:mskc_ref_compnr::integer AS mskc_ref_compnr, 
                        src:omth::varchar AS omth, 
                        src:omth_ref_compnr::integer AS omth_ref_compnr, 
                        src:prop::varchar AS prop, 
                        src:prop_ref_compnr::integer AS prop_ref_compnr, 
                        src:sctg::varchar AS sctg, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:smth::varchar AS smth, 
                        src:smth_ref_compnr::integer AS smth_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tmth::varchar AS tmth, 
                        src:tmth_ref_compnr::integer AS tmth_ref_compnr, 
                        src:umth::varchar AS umth, 
                        src:umth_ref_compnr::integer AS umth_ref_compnr, 
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
    
                        
                src:ctgy,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ctgy,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM200 as strm))