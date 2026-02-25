CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM650 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:cpga::integer AS cpga, 
                        src:cpga_kw::varchar AS cpga_kw, 
                        src:czev::integer AS czev, 
                        src:czev_kw::varchar AS czev_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:impa::integer AS impa, 
                        src:impa_kw::varchar AS impa_kw, 
                        src:reas::varchar AS reas, 
                        src:rety::integer AS rety, 
                        src:rety_kw::varchar AS rety_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:skev::integer AS skev, 
                        src:skev_kw::varchar AS skev_kw, 
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
                src:reas,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:reas  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM650 as strm))