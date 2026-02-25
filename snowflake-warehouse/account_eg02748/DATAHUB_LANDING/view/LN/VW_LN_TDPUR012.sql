CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDPUR012 AS SELECT
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:ngco::varchar AS ngco, 
                        src:ngco_ref_compnr::integer AS ngco_ref_compnr, 
                        src:ngco_scid_ref_compnr::integer AS ngco_scid_ref_compnr, 
                        src:ngpc::varchar AS ngpc, 
                        src:ngpc_ref_compnr::integer AS ngpc_ref_compnr, 
                        src:ngpc_sepc_ref_compnr::integer AS ngpc_sepc_ref_compnr, 
                        src:ngpo::varchar AS ngpo, 
                        src:ngpo_ref_compnr::integer AS ngpo_ref_compnr, 
                        src:ngpo_sepo_ref_compnr::integer AS ngpo_sepo_ref_compnr, 
                        src:ngpo_seps_ref_compnr::integer AS ngpo_seps_ref_compnr, 
                        src:ngpq::varchar AS ngpq, 
                        src:ngpq_ref_compnr::integer AS ngpq_ref_compnr, 
                        src:ngpq_sepq_ref_compnr::integer AS ngpq_sepq_ref_compnr, 
                        src:ngpr::varchar AS ngpr, 
                        src:ngpr_ref_compnr::integer AS ngpr_ref_compnr, 
                        src:ngpr_sepr_ref_compnr::integer AS ngpr_sepr_ref_compnr, 
                        src:ngrl::varchar AS ngrl, 
                        src:ngrl_ref_compnr::integer AS ngrl_ref_compnr, 
                        src:ngrl_serl_ref_compnr::integer AS ngrl_serl_ref_compnr, 
                        src:ngsp::varchar AS ngsp, 
                        src:ngsp_ref_compnr::integer AS ngsp_ref_compnr, 
                        src:ngsp_safp_ref_compnr::integer AS ngsp_safp_ref_compnr, 
                        src:ngsp_seqo_ref_compnr::integer AS ngsp_seqo_ref_compnr, 
                        src:ngsp_sequ_ref_compnr::integer AS ngsp_sequ_ref_compnr, 
                        src:ngsp_sspo_ref_compnr::integer AS ngsp_sspo_ref_compnr, 
                        src:safp::varchar AS safp, 
                        src:scid::varchar AS scid, 
                        src:sepc::varchar AS sepc, 
                        src:sepo::varchar AS sepo, 
                        src:sepq::varchar AS sepq, 
                        src:sepr::varchar AS sepr, 
                        src:seps::varchar AS seps, 
                        src:seqo::varchar AS seqo, 
                        src:sequ::varchar AS sequ, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serl::varchar AS serl, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:sspo::varchar AS sspo, 
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
    
                        
                src:compnr,
                src:cofc,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDPUR012 as strm))