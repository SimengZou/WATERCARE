CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM808 AS SELECT
                        src:aext::varchar AS aext, 
                        src:anbr::varchar AS anbr, 
                        src:book::varchar AS book, 
                        src:compnr::integer AS compnr, 
                        src:cost_1::numeric(38, 17) AS cost_1, 
                        src:cost_2::numeric(38, 17) AS cost_2, 
                        src:cost_3::numeric(38, 17) AS cost_3, 
                        src:deleted::boolean AS deleted, 
                        src:ltdd_1::numeric(38, 17) AS ltdd_1, 
                        src:ltdd_2::numeric(38, 17) AS ltdd_2, 
                        src:ltdd_3::numeric(38, 17) AS ltdd_3, 
                        src:ltdr_1::numeric(38, 17) AS ltdr_1, 
                        src:ltdr_2::numeric(38, 17) AS ltdr_2, 
                        src:ltdr_3::numeric(38, 17) AS ltdr_3, 
                        src:ocst_1::numeric(38, 17) AS ocst_1, 
                        src:ocst_2::numeric(38, 17) AS ocst_2, 
                        src:ocst_3::numeric(38, 17) AS ocst_3, 
                        src:prod::integer AS prod, 
                        src:rcst_1::numeric(38, 17) AS rcst_1, 
                        src:rcst_2::numeric(38, 17) AS rcst_2, 
                        src:rcst_3::numeric(38, 17) AS rcst_3, 
                        src:s179_1::numeric(38, 17) AS s179_1, 
                        src:s179_2::numeric(38, 17) AS s179_2, 
                        src:s179_3::numeric(38, 17) AS s179_3, 
                        src:salv_1::numeric(38, 17) AS salv_1, 
                        src:salv_2::numeric(38, 17) AS salv_2, 
                        src:salv_3::numeric(38, 17) AS salv_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:susp::integer AS susp, 
                        src:susp_kw::varchar AS susp_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, 
                        src:ytdd_1::numeric(38, 17) AS ytdd_1, 
                        src:ytdd_2::numeric(38, 17) AS ytdd_2, 
                        src:ytdd_3::numeric(38, 17) AS ytdd_3, 
                        src:ytdr_1::numeric(38, 17) AS ytdr_1, 
                        src:ytdr_2::numeric(38, 17) AS ytdr_2, 
                        src:ytdr_3::numeric(38, 17) AS ytdr_3, 
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
                src:year,
                src:anbr,
                src:aext,
                src:prod,
                src:book,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:year,
                src:anbr,
                src:aext,
                src:prod,
                src:book  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM808 as strm))