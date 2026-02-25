CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TTDPM530 AS SELECT
                        src:burl::varchar AS burl, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:llid::varchar AS llid, 
                        src:loid::varchar AS loid, 
                        src:oack::varchar AS oack, 
                        src:oask::varchar AS oask, 
                        src:pmch::integer AS pmch, 
                        src:pmch_kw::varchar AS pmch_kw, 
                        src:pmil::integer AS pmil, 
                        src:pmil_kw::varchar AS pmil_kw, 
                        src:rack::varchar AS rack, 
                        src:rask::varchar AS rask, 
                        src:rurl::varchar AS rurl, 
                        src:sequ::integer AS sequ, 
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
    
                        
                src:sequ,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:sequ,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TTDPM530 as strm))