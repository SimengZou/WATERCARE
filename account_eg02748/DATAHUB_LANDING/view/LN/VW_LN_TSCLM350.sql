CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCLM350 AS SELECT
                        src:acln::integer AS acln, 
                        src:ccll::varchar AS ccll, 
                        src:ccll_ref_compnr::integer AS ccll_ref_compnr, 
                        src:cgrp::varchar AS cgrp, 
                        src:cgrp_ref_compnr::integer AS cgrp_ref_compnr, 
                        src:clst::varchar AS clst, 
                        src:clst_ref_compnr::integer AS clst_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crac::varchar AS crac, 
                        src:crac_ref_compnr::integer AS crac_ref_compnr, 
                        src:csgr::varchar AS csgr, 
                        src:csgr_ref_compnr::integer AS csgr_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:espr::varchar AS espr, 
                        src:espr_ref_compnr::integer AS espr_ref_compnr, 
                        src:expr::varchar AS expr, 
                        src:expr_ref_compnr::integer AS expr_ref_compnr, 
                        src:exsl::varchar AS exsl, 
                        src:exsl_ref_compnr::integer AS exsl_ref_compnr, 
                        src:hidt::datetime AS hidt, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:item_sern_ref_compnr::integer AS item_sern_ref_compnr, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:orig::integer AS orig, 
                        src:orig_kw::varchar AS orig_kw, 
                        src:orno::varchar AS orno, 
                        src:rprl::varchar AS rprl, 
                        src:rprl_ref_compnr::integer AS rprl_ref_compnr, 
                        src:scgr::varchar AS scgr, 
                        src:scgr_ref_compnr::integer AS scgr_ref_compnr, 
                        src:sear::varchar AS sear, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::varchar AS sern, 
                        src:sigr::varchar AS sigr, 
                        src:sigr_ref_compnr::integer AS sigr_ref_compnr, 
                        src:sltn::varchar AS sltn, 
                        src:sltn_ref_compnr::integer AS sltn_ref_compnr, 
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
    
                        
                src:orno,
                src:compnr,
                src:ccll,
                src:acln,
                src:orig,
                src:hidt,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:orno,
                src:compnr,
                src:ccll,
                src:acln,
                src:orig,
                src:hidt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCLM350 as strm))