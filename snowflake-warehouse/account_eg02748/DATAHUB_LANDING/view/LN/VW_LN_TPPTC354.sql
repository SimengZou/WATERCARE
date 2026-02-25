CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPTC354 AS SELECT
                        src:amch_1::numeric(38, 17) AS amch_1, 
                        src:amch_2::numeric(38, 17) AS amch_2, 
                        src:amch_3::numeric(38, 17) AS amch_3, 
                        src:amch_4::numeric(38, 17) AS amch_4, 
                        src:ames_1::numeric(38, 17) AS ames_1, 
                        src:ames_2::numeric(38, 17) AS ames_2, 
                        src:ames_3::numeric(38, 17) AS ames_3, 
                        src:ames_4::numeric(38, 17) AS ames_4, 
                        src:ccal::varchar AS ccal, 
                        src:ccic::varchar AS ccic, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ccal_ref_compnr::integer AS cprj_ccal_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:quan::numeric(38, 17) AS quan, 
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
    
                        
                src:compnr,
                src:ccic,
                src:ccal,
                src:cprj,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:ccic,
                src:ccal,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPTC354 as strm))