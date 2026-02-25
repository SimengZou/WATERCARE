CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TIROU101 AS SELECT
                        src:bwca::varchar AS bwca, 
                        src:bwca_ref_compnr::integer AS bwca_ref_compnr, 
                        src:bwce::varchar AS bwce, 
                        src:bwce_ref_compnr::integer AS bwce_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:lcdr::datetime AS lcdr, 
                        src:lmdt::datetime AS lmdt, 
                        src:mitm::varchar AS mitm, 
                        src:mitm_ref_compnr::integer AS mitm_ref_compnr, 
                        src:mrau::integer AS mrau, 
                        src:mrau_kw::varchar AS mrau_kw, 
                        src:opro::varchar AS opro, 
                        src:ract::numeric(38, 17) AS ract, 
                        src:rcal::numeric(38, 17) AS rcal, 
                        src:runi::numeric(38, 17) AS runi, 
                        src:ruoq::numeric(38, 17) AS ruoq, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stcf::integer AS stcf, 
                        src:stcf_kw::varchar AS stcf_kw, 
                        src:stor::integer AS stor, 
                        src:stor_kw::varchar AS stor_kw, 
                        src:strc::varchar AS strc, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:unef::integer AS unef, 
                        src:unef_kw::varchar AS unef_kw, 
                        src:username::varchar AS username, 
                        src:wkbt::integer AS wkbt, 
                        src:wkbt_kw::varchar AS wkbt_kw, 
                        src:woar::varchar AS woar, 
                        src:woar_ref_compnr::integer AS woar_ref_compnr, 
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
    
                        
                src:opro,
                src:compnr,
                src:mitm,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:opro,
                src:compnr,
                src:mitm  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TIROU101 as strm))