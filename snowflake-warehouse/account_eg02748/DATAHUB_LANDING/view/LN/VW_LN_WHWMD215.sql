CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHWMD215 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_item_ref_compnr::integer AS cwar_item_ref_compnr, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:hstd::datetime AS hstd, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:lsid::datetime AS lsid, 
                        src:ltdt::datetime AS ltdt, 
                        src:qall::numeric(38, 17) AS qall, 
                        src:qblk::numeric(38, 17) AS qblk, 
                        src:qbpl::numeric(38, 17) AS qbpl, 
                        src:qcal::numeric(38, 17) AS qcal, 
                        src:qchd::numeric(38, 17) AS qchd, 
                        src:qcis::numeric(38, 17) AS qcis, 
                        src:qcit::numeric(38, 17) AS qcit, 
                        src:qcom::numeric(38, 17) AS qcom, 
                        src:qcor::numeric(38, 17) AS qcor, 
                        src:qcpr::numeric(38, 17) AS qcpr, 
                        src:qcrj::numeric(38, 17) AS qcrj, 
                        src:qgit::numeric(38, 17) AS qgit, 
                        src:qhib::numeric(38, 17) AS qhib, 
                        src:qhnd::numeric(38, 17) AS qhnd, 
                        src:qhrj::numeric(38, 17) AS qhrj, 
                        src:qint::numeric(38, 17) AS qint, 
                        src:qlal::numeric(38, 17) AS qlal, 
                        src:qnal::numeric(38, 17) AS qnal, 
                        src:qnbl::numeric(38, 17) AS qnbl, 
                        src:qnbp::numeric(38, 17) AS qnbp, 
                        src:qnhd::numeric(38, 17) AS qnhd, 
                        src:qnit::numeric(38, 17) AS qnit, 
                        src:qnor::numeric(38, 17) AS qnor, 
                        src:qnrj::numeric(38, 17) AS qnrj, 
                        src:qoal::numeric(38, 17) AS qoal, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:qord::numeric(38, 17) AS qord, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
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
                src:cwar,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:item,
                src:cwar,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHWMD215 as strm))