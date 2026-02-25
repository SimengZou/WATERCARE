CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TIPCS020 AS SELECT
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:ccgr::varchar AS ccgr, 
                        src:ccgr_ref_compnr::integer AS ccgr_ref_compnr, 
                        src:cfdt::datetime AS cfdt, 
                        src:clco::varchar AS clco, 
                        src:clco_ref_compnr::integer AS clco_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpcc::varchar AS cpcc, 
                        src:cpcc_ref_compnr::integer AS cpcc_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dtfs::integer AS dtfs, 
                        src:dtfs_kw::varchar AS dtfs_kw, 
                        src:ffci::integer AS ffci, 
                        src:kopr::integer AS kopr, 
                        src:kopr_kw::varchar AS kopr_kw, 
                        src:pemp::varchar AS pemp, 
                        src:pemp_ref_compnr::integer AS pemp_ref_compnr, 
                        src:peng::integer AS peng, 
                        src:peng_kw::varchar AS peng_kw, 
                        src:refe::object AS refe, 
                        src:seak::object AS seak, 
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
    
                        
                src:cprj,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TIPCS020 as strm))