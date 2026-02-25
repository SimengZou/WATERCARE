CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDISA001 AS SELECT
                        src:acci::integer AS acci, 
                        src:acci_kw::varchar AS acci_kw, 
                        src:acom::integer AS acom, 
                        src:acom_kw::varchar AS acom_kw, 
                        src:actn::integer AS actn, 
                        src:actn_kw::varchar AS actn_kw, 
                        src:afrb::integer AS afrb, 
                        src:afrb_kw::varchar AS afrb_kw, 
                        src:alod::integer AS alod, 
                        src:alod_kw::varchar AS alod_kw, 
                        src:bfbp::varchar AS bfbp, 
                        src:bfbp_ref_compnr::integer AS bfbp_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cmgp::varchar AS cmgp, 
                        src:cmgp_ref_compnr::integer AS cmgp_ref_compnr, 
                        src:cncd::varchar AS cncd, 
                        src:compnr::integer AS compnr, 
                        src:cpgs::varchar AS cpgs, 
                        src:cpgs_ref_compnr::integer AS cpgs_ref_compnr, 
                        src:cphl::integer AS cphl, 
                        src:cphl_kw::varchar AS cphl_kw, 
                        src:csgs::varchar AS csgs, 
                        src:csgs_ref_compnr::integer AS csgs_ref_compnr, 
                        src:cups::varchar AS cups, 
                        src:cups_ref_compnr::integer AS cups_ref_compnr, 
                        src:cuqi::varchar AS cuqi, 
                        src:cuqi_ref_compnr::integer AS cuqi_ref_compnr, 
                        src:cuqs::varchar AS cuqs, 
                        src:cuqs_ref_compnr::integer AS cuqs_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:item_sfsi_ref_compnr::integer AS item_sfsi_ref_compnr, 
                        src:lcmp::integer AS lcmp, 
                        src:lcmp_ref_compnr::integer AS lcmp_ref_compnr, 
                        src:lmsp::numeric(38, 17) AS lmsp, 
                        src:ltsp::datetime AS ltsp, 
                        src:mmtl::numeric(38, 17) AS mmtl, 
                        src:mmtp::numeric(38, 17) AS mmtp, 
                        src:mnra::numeric(38, 17) AS mnra, 
                        src:prir::numeric(38, 17) AS prir, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:pris_dtwc::numeric(38, 17) AS pris_dtwc, 
                        src:pris_lclc::numeric(38, 17) AS pris_lclc, 
                        src:pris_rfrc::numeric(38, 17) AS pris_rfrc, 
                        src:pris_rpc1::numeric(38, 17) AS pris_rpc1, 
                        src:pris_rpc2::numeric(38, 17) AS pris_rpc2, 
                        src:qidd::numeric(38, 17) AS qidd, 
                        src:qimc::numeric(38, 17) AS qimc, 
                        src:qimo::numeric(38, 17) AS qimo, 
                        src:qpdd::numeric(38, 17) AS qpdd, 
                        src:qpmo::numeric(38, 17) AS qpmo, 
                        src:rbgp::varchar AS rbgp, 
                        src:rbgp_ref_compnr::integer AS rbgp_ref_compnr, 
                        src:rcom::integer AS rcom, 
                        src:rcom_kw::varchar AS rcom_kw, 
                        src:retw::integer AS retw, 
                        src:retw_kw::varchar AS retw_kw, 
                        src:scon::integer AS scon, 
                        src:scon_kw::varchar AS scon_kw, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfbp::varchar AS sfbp, 
                        src:sfbp_ref_compnr::integer AS sfbp_ref_compnr, 
                        src:sfsi::varchar AS sfsi, 
                        src:sfsi_ref_compnr::integer AS sfsi_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tltp::integer AS tltp, 
                        src:tltp_kw::varchar AS tltp_kw, 
                        src:txts::integer AS txts, 
                        src:txts_ref_compnr::integer AS txts_ref_compnr, 
                        src:umsp::numeric(38, 17) AS umsp, 
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
    
                        
                src:item,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:item,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDISA001 as strm))