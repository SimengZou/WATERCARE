CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS000 AS SELECT
                        src:care::varchar AS care, 
                        src:care_ref_compnr::integer AS care_ref_compnr, 
                        src:clen::varchar AS clen, 
                        src:clen_ref_compnr::integer AS clen_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:ctim::varchar AS ctim, 
                        src:ctim_ref_compnr::integer AS ctim_ref_compnr, 
                        src:cvol::varchar AS cvol, 
                        src:cvol_ref_compnr::integer AS cvol_ref_compnr, 
                        src:cwei::varchar AS cwei, 
                        src:cwei_ref_compnr::integer AS cwei_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:indt::datetime AS indt, 
                        src:logn::varchar AS logn, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
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
                src:indt,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:indt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS000 as strm))