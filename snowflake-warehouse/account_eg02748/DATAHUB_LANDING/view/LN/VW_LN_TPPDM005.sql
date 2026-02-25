CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPDM005 AS SELECT
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:ccfu::integer AS ccfu, 
                        src:ccfu_kw::varchar AS ccfu_kw, 
                        src:ccit::varchar AS ccit, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:copt::integer AS copt, 
                        src:copt_kw::varchar AS copt_kw, 
                        src:cpmc_1::numeric(38, 17) AS cpmc_1, 
                        src:cpmc_2::numeric(38, 17) AS cpmc_2, 
                        src:cpmc_3::numeric(38, 17) AS cpmc_3, 
                        src:cppp::integer AS cppp, 
                        src:cppp_kw::varchar AS cppp_kw, 
                        src:cprp::numeric(38, 17) AS cprp, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:ltcp::datetime AS ltcp, 
                        src:osys::integer AS osys, 
                        src:osys_kw::varchar AS osys_kw, 
                        src:pgwh::integer AS pgwh, 
                        src:pgwh_kw::varchar AS pgwh_kw, 
                        src:prre::integer AS prre, 
                        src:prre_kw::varchar AS prre_kw, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:usyn::integer AS usyn, 
                        src:usyn_kw::varchar AS usyn_kw, 
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
                src:item,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPDM005 as strm))