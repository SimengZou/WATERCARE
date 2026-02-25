CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDSMI113 AS SELECT
                        src:amta::numeric(38, 17) AS amta, 
                        src:amta_dtwc::numeric(38, 17) AS amta_dtwc, 
                        src:amta_lclc::numeric(38, 17) AS amta_lclc, 
                        src:amta_rfrc::numeric(38, 17) AS amta_rfrc, 
                        src:amta_rpc1::numeric(38, 17) AS amta_rpc1, 
                        src:amta_rpc2::numeric(38, 17) AS amta_rpc2, 
                        src:amtg_dtwc::numeric(38, 17) AS amtg_dtwc, 
                        src:amtg_lclc::numeric(38, 17) AS amtg_lclc, 
                        src:amtg_rfrc::numeric(38, 17) AS amtg_rfrc, 
                        src:amtg_rpc1::numeric(38, 17) AS amtg_rpc1, 
                        src:amtg_rpc2::numeric(38, 17) AS amtg_rpc2, 
                        src:amtg_trnc::numeric(38, 17) AS amtg_trnc, 
                        src:cact::varchar AS cact, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cuqs::varchar AS cuqs, 
                        src:cuqs_ref_compnr::integer AS cuqs_ref_compnr, 
                        src:cvqs::numeric(38, 17) AS cvqs, 
                        src:deleted::boolean AS deleted, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:opty::varchar AS opty, 
                        src:opty_ref_compnr::integer AS opty_ref_compnr, 
                        src:pono::integer AS pono, 
                        src:prid::varchar AS prid, 
                        src:qoor::numeric(38, 17) AS qoor, 
                        src:qoor_buar::numeric(38, 17) AS qoor_buar, 
                        src:qoor_buln::numeric(38, 17) AS qoor_buln, 
                        src:qoor_bupc::numeric(38, 17) AS qoor_bupc, 
                        src:qoor_butm::numeric(38, 17) AS qoor_butm, 
                        src:qoor_buvl::numeric(38, 17) AS qoor_buvl, 
                        src:qoor_buwg::numeric(38, 17) AS qoor_buwg, 
                        src:qoor_invu::numeric(38, 17) AS qoor_invu, 
                        src:seli::integer AS seli, 
                        src:seli_kw::varchar AS seli_kw, 
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
    
                        
                src:opty,
                src:pono,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:opty,
                src:pono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDSMI113 as strm))