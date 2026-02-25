CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINA116 AS SELECT
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:cbyi::integer AS cbyi, 
                        src:cbyi_kw::varchar AS cbyi_kw, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cuso::integer AS cuso, 
                        src:cuso_kw::varchar AS cuso_kw, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:depc::integer AS depc, 
                        src:depc_ref_compnr::integer AS depc_ref_compnr, 
                        src:dlin::integer AS dlin, 
                        src:iror::integer AS iror, 
                        src:iror_kw::varchar AS iror_kw, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:ivsq::integer AS ivsq, 
                        src:koor::integer AS koor, 
                        src:koor_kw::varchar AS koor_kw, 
                        src:lgdt::datetime AS lgdt, 
                        src:orno::varchar AS orno, 
                        src:owns::integer AS owns, 
                        src:owns_kw::varchar AS owns_kw, 
                        src:pono::integer AS pono, 
                        src:prdt::datetime AS prdt, 
                        src:proc::integer AS proc, 
                        src:proc_kw::varchar AS proc_kw, 
                        src:pyps::integer AS pyps, 
                        src:rcln::integer AS rcln, 
                        src:rcno::varchar AS rcno, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:srnb::integer AS srnb, 
                        src:stkq::numeric(38, 17) AS stkq, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:username::varchar AS username, 
                        src:wdep::varchar AS wdep, 
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
    
                        
                src:compnr,
                src:seqn,
                src:cwar,
                src:item,
                src:trdt,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:seqn,
                src:cwar,
                src:item,
                src:trdt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINA116 as strm))