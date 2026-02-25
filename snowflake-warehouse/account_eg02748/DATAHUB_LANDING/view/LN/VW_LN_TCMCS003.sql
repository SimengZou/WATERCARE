CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS003 AS SELECT
                        src:bpid::varchar AS bpid, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:casi::varchar AS casi, 
                        src:casi_ref_compnr::integer AS casi_ref_compnr, 
                        src:cdf_updt::datetime AS cdf_updt, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:comp::integer AS comp, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_eunt::varchar AS cwar_eunt, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:duns::integer AS duns, 
                        src:eunt_rcmp::integer AS eunt_rcmp, 
                        src:imbp::varchar AS imbp, 
                        src:imbp_ref_compnr::integer AS imbp_ref_compnr, 
                        src:imgt::integer AS imgt, 
                        src:imgt_kw::varchar AS imgt_kw, 
                        src:inep::integer AS inep, 
                        src:inep_kw::varchar AS inep_kw, 
                        src:mesc::integer AS mesc, 
                        src:mesc_kw::varchar AS mesc_kw, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:pwip::integer AS pwip, 
                        src:pwip_kw::varchar AS pwip_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfbp::varchar AS sfbp, 
                        src:sfbp_ref_compnr::integer AS sfbp_ref_compnr, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:swhu::integer AS swhu, 
                        src:swhu_kw::varchar AS swhu_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:typw::integer AS typw, 
                        src:typw_kw::varchar AS typw_kw, 
                        src:username::varchar AS username, 
                        src:wmsc::integer AS wmsc, 
                        src:wmsc_kw::varchar AS wmsc_kw, 
                        src:xsbp::varchar AS xsbp, 
                        src:xsbp_ref_compnr::integer AS xsbp_ref_compnr, 
                        src:xsit::integer AS xsit, 
                        src:xsit_kw::varchar AS xsit_kw, 
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
                src:cwar,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cwar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS003 as strm))