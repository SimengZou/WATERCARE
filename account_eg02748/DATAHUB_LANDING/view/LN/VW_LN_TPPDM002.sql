CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPDM002 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:loco::varchar AS loco, 
                        src:maps::integer AS maps, 
                        src:maps_kw::varchar AS maps_kw, 
                        src:mpss::integer AS mpss, 
                        src:mpss_kw::varchar AS mpss_kw, 
                        src:mpvw::integer AS mpvw, 
                        src:mpvw_kw::varchar AS mpvw_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sses::integer AS sses, 
                        src:sses_kw::varchar AS sses_kw, 
                        src:ssvw::integer AS ssvw, 
                        src:ssvw_kw::varchar AS ssvw_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tprf::integer AS tprf, 
                        src:tprf_kw::varchar AS tprf_kw, 
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
    
                        
                src:maps,
                src:loco,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:maps,
                src:loco,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPDM002 as strm))