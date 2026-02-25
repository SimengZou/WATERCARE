CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP260 AS SELECT
                        src:byer::varchar AS byer, 
                        src:clus::varchar AS clus, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:idoc::integer AS idoc, 
                        src:ityp::varchar AS ityp, 
                        src:load::varchar AS load, 
                        src:loco::integer AS loco, 
                        src:logd::date AS logd, 
                        src:logt::integer AS logt, 
                        src:mess::object AS mess, 
                        src:orno::varchar AS orno, 
                        src:otyp::integer AS otyp, 
                        src:otyp_kw::varchar AS otyp_kw, 
                        src:pono::integer AS pono, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shpi::varchar AS shpi, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:user::varchar AS user, 
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
                                        
                src:ityp,
                src:idoc,
                src:orno,
                src:loco,
                src:compnr,
                src:pono,
                src:sqnb,
                src:otyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP260 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:logd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:logt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null