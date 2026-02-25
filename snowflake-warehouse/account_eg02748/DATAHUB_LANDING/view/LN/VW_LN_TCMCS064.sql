CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS064 AS SELECT
                        src:actn::integer AS actn, 
                        src:actn_kw::varchar AS actn_kw, 
                        src:compnr::integer AS compnr, 
                        src:crat::varchar AS crat, 
                        src:ddcc::integer AS ddcc, 
                        src:ddcc_kw::varchar AS ddcc_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:ioso::integer AS ioso, 
                        src:ioso_kw::varchar AS ioso_kw, 
                        src:phs1::integer AS phs1, 
                        src:phs1_kw::varchar AS phs1_kw, 
                        src:phs2::integer AS phs2, 
                        src:phs2_kw::varchar AS phs2_kw, 
                        src:phs3::integer AS phs3, 
                        src:phs3_kw::varchar AS phs3_kw, 
                        src:phs4::integer AS phs4, 
                        src:phs4_kw::varchar AS phs4_kw, 
                        src:phs5::integer AS phs5, 
                        src:phs5_kw::varchar AS phs5_kw, 
                        src:phs6::integer AS phs6, 
                        src:phs6_kw::varchar AS phs6_kw, 
                        src:phs7::integer AS phs7, 
                        src:phs7_kw::varchar AS phs7_kw, 
                        src:revp::integer AS revp, 
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
    
                        
                src:crat,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:crat,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS064 as strm))