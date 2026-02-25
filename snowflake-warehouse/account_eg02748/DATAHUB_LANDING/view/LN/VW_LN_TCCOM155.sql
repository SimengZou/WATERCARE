CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCOM155 AS SELECT
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:cert::object AS cert, 
                        src:code::varchar AS code, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:efdt::datetime AS efdt, 
                        src:exdt::datetime AS exdt, 
                        src:kcod::integer AS kcod, 
                        src:kcod_kw::varchar AS kcod_kw, 
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
    
                        
                src:code,
                src:kcod,
                src:cofc,
                src:compnr,
                src:bpid,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:code,
                src:kcod,
                src:cofc,
                src:compnr,
                src:bpid  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCOM155 as strm))