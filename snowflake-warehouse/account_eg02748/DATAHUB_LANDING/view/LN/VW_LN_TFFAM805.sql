CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM805 AS SELECT
                        src:aext::varchar AS aext, 
                        src:anbr::varchar AS anbr, 
                        src:book::varchar AS book, 
                        src:bpid::varchar AS bpid, 
                        src:compnr::integer AS compnr, 
                        src:cost_1::numeric(38, 17) AS cost_1, 
                        src:cost_2::numeric(38, 17) AS cost_2, 
                        src:cost_3::numeric(38, 17) AS cost_3, 
                        src:deleted::boolean AS deleted, 
                        src:depr_1::numeric(38, 17) AS depr_1, 
                        src:depr_2::numeric(38, 17) AS depr_2, 
                        src:depr_3::numeric(38, 17) AS depr_3, 
                        src:lkey::integer AS lkey, 
                        src:rcst_1::numeric(38, 17) AS rcst_1, 
                        src:rcst_2::numeric(38, 17) AS rcst_2, 
                        src:rcst_3::numeric(38, 17) AS rcst_3, 
                        src:rdpr_1::numeric(38, 17) AS rdpr_1, 
                        src:rdpr_2::numeric(38, 17) AS rdpr_2, 
                        src:rdpr_3::numeric(38, 17) AS rdpr_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:tkey::integer AS tkey, 
                        src:tkey_ref_compnr::integer AS tkey_ref_compnr, 
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
    
                        
                src:book,
                src:lkey,
                src:anbr,
                src:compnr,
                src:tkey,
                src:bpid,
                src:aext,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:book,
                src:lkey,
                src:anbr,
                src:compnr,
                src:tkey,
                src:bpid,
                src:aext  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM805 as strm))