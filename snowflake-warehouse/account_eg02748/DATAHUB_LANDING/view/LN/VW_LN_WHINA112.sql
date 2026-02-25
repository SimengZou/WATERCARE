CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINA112 AS SELECT
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:bfbp::varchar AS bfbp, 
                        src:clot::varchar AS clot, 
                        src:compnr::integer AS compnr, 
                        src:cons::integer AS cons, 
                        src:cons_kw::varchar AS cons_kw, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dlin::integer AS dlin, 
                        src:inwp::integer AS inwp, 
                        src:inwp_kw::varchar AS inwp_kw, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:itid::varchar AS itid, 
                        src:itse::integer AS itse, 
                        src:koor::integer AS koor, 
                        src:koor_kw::varchar AS koor_kw, 
                        src:lgdt::datetime AS lgdt, 
                        src:ocmp::integer AS ocmp, 
                        src:ocmp_ref_compnr::integer AS ocmp_ref_compnr, 
                        src:orno::varchar AS orno, 
                        src:ownr::varchar AS ownr, 
                        src:owns::integer AS owns, 
                        src:owns_kw::varchar AS owns_kw, 
                        src:phtr::integer AS phtr, 
                        src:phtr_kw::varchar AS phtr_kw, 
                        src:pono::integer AS pono, 
                        src:qaiu::numeric(38, 17) AS qaiu, 
                        src:qhnd::numeric(38, 17) AS qhnd, 
                        src:qskt::numeric(38, 17) AS qskt, 
                        src:qstk::numeric(38, 17) AS qstk, 
                        src:reje::integer AS reje, 
                        src:reje_kw::varchar AS reje_kw, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serl::varchar AS serl, 
                        src:srnb::integer AS srnb, 
                        src:tagn::varchar AS tagn, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:username::varchar AS username, 
                        src:vaco::integer AS vaco, 
                        src:vaco_kw::varchar AS vaco_kw, 
                        src:wvgr::varchar AS wvgr, 
                        src:wvgr_ref_compnr::integer AS wvgr_ref_compnr, 
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
    
                        
                src:inwp,
                src:cwar,
                src:trdt,
                src:compnr,
                src:seqn,
                src:item,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:inwp,
                src:cwar,
                src:trdt,
                src:compnr,
                src:seqn,
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINA112 as strm))