CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCOM101 AS SELECT
                        src:bprl::integer AS bprl, 
                        src:bprl_kw::varchar AS bprl_kw, 
                        src:btbv::integer AS btbv, 
                        src:btbv_kw::varchar AS btbv_kw, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cfcg::varchar AS cfcg, 
                        src:cfir::integer AS cfir, 
                        src:cfir_kw::varchar AS cfir_kw, 
                        src:cfsg::varchar AS cfsg, 
                        src:cinm::varchar AS cinm, 
                        src:cinm_ref_compnr::integer AS cinm_ref_compnr, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:crat::varchar AS crat, 
                        src:crat_ref_compnr::integer AS crat_ref_compnr, 
                        src:ctyp::varchar AS ctyp, 
                        src:ctyp_ref_compnr::integer AS ctyp_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:lmdt::datetime AS lmdt, 
                        src:pcur::varchar AS pcur, 
                        src:pcur_ref_compnr::integer AS pcur_ref_compnr, 
                        src:scur::varchar AS scur, 
                        src:scur_ref_compnr::integer AS scur_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:sfir::integer AS sfir, 
                        src:sfir_kw::varchar AS sfir_kw, 
                        src:spay::varchar AS spay, 
                        src:spay_ref_compnr::integer AS spay_ref_compnr, 
                        src:styp::varchar AS styp, 
                        src:styp_ref_compnr::integer AS styp_ref_compnr, 
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
    
                        
                src:sern,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:sern,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCOM101 as strm))