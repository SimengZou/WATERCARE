CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS005 AS SELECT
                        src:appr::integer AS appr, 
                        src:appr_kw::varchar AS appr_kw, 
                        src:bilb::integer AS bilb, 
                        src:bilb_kw::varchar AS bilb_kw, 
                        src:cdis::varchar AS cdis, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:drpi::integer AS drpi, 
                        src:drpi_kw::varchar AS drpi_kw, 
                        src:dsca::object AS dsca, 
                        src:efdt::datetime AS efdt, 
                        src:etrt::integer AS etrt, 
                        src:etrt_kw::varchar AS etrt_kw, 
                        src:exdt::datetime AS exdt, 
                        src:rstp::integer AS rstp, 
                        src:rstp_kw::varchar AS rstp_kw, 
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
    
                        
                src:cdis,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cdis,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS005 as strm))