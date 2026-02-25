CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINH600 AS SELECT
                        src:boml::integer AS boml, 
                        src:cdor::varchar AS cdor, 
                        src:cdos::integer AS cdos, 
                        src:cdos_kw::varchar AS cdos_kw, 
                        src:cdpd::varchar AS cdpd, 
                        src:cdpd_ref_compnr::integer AS cdpd_ref_compnr, 
                        src:cdty::integer AS cdty, 
                        src:cdty_kw::varchar AS cdty_kw, 
                        src:cdun::varchar AS cdun, 
                        src:cdun_ref_compnr::integer AS cdun_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:cwar_stlo_ref_compnr::integer AS cwar_stlo_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dlin::integer AS dlin, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:lcdt::datetime AS lcdt, 
                        src:oorg::integer AS oorg, 
                        src:oorg_kw::varchar AS oorg_kw, 
                        src:orno::varchar AS orno, 
                        src:oset::integer AS oset, 
                        src:pddt::datetime AS pddt, 
                        src:pono::integer AS pono, 
                        src:qact::numeric(38, 17) AS qact, 
                        src:qadv::numeric(38, 17) AS qadv, 
                        src:qapp::numeric(38, 17) AS qapp, 
                        src:qpla::numeric(38, 17) AS qpla, 
                        src:qreq::numeric(38, 17) AS qreq, 
                        src:qrun::numeric(38, 17) AS qrun, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stlo::varchar AS stlo, 
                        src:sypr::varchar AS sypr, 
                        src:timestamp::datetime AS timestamp, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:uspr::integer AS uspr, 
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
    
                        
                src:cdor,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cdor,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINH600 as strm))