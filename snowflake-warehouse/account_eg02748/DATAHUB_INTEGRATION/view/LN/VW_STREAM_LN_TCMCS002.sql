CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS002 AS SELECT
                        src:brep::integer AS brep, 
                        src:brep_kw::varchar AS brep_kw, 
                        src:ccur::varchar AS ccur, 
                        src:compnr::integer AS compnr, 
                        src:crnd::numeric(38, 17) AS crnd, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:dscb::object AS dscb, 
                        src:emuc::integer AS emuc, 
                        src:emuc_kw::varchar AS emuc_kw, 
                        src:gtrf::numeric(38, 17) AS gtrf, 
                        src:iccc::varchar AS iccc, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:ccur  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCMCS002 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:brep), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crnd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:emuc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gtrf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null