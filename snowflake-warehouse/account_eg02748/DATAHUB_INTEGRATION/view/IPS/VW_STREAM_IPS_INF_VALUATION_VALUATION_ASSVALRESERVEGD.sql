CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD AS SELECT
                        src:ACUMDEPB4REVAL::numeric(38, 10) AS ACUMDEPB4REVAL, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALRESERVEGDKEY::integer AS ASSVALRESERVEGDKEY, 
                        src:ASSVALRESERVEKEY::integer AS ASSVALRESERVEKEY, 
                        src:BALSHTBOOKVAL::numeric(38, 10) AS BALSHTBOOKVAL, 
                        src:CAPEXP::numeric(38, 10) AS CAPEXP, 
                        src:CLOSERESERVE::numeric(38, 10) AS CLOSERESERVE, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPDATE::datetime AS DISPDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MOVEMENT::numeric(38, 10) AS MOVEMENT, 
                        src:OPENEXPLIFE::numeric(38, 10) AS OPENEXPLIFE, 
                        src:OPENFYDATE::datetime AS OPENFYDATE, 
                        src:OPENREPLCOST::numeric(38, 10) AS OPENREPLCOST, 
                        src:OPENRESERVE::numeric(38, 10) AS OPENRESERVE, 
                        src:OPENRESIDLIFE::numeric(38, 10) AS OPENRESIDLIFE, 
                        src:OPENWDV::numeric(38, 10) AS OPENWDV, 
                        src:REMLIFE::numeric(38, 10) AS REMLIFE, 
                        src:RESERVEDATE::datetime AS RESERVEDATE, 
                        src:REVALREPLCOST::numeric(38, 10) AS REVALREPLCOST, 
                        src:RUNNO::integer AS RUNNO, 
                        src:SOURCETYPE::varchar AS SOURCETYPE, 
                        src:VALKEY::integer AS VALKEY, 
                        src:VALUETODISPOSE::numeric(38, 10) AS VALUETODISPOSE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WDV::numeric(38, 10) AS WDV, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
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
                                        
                src:ASSVALRESERVEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACUMDEPB4REVAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALRESERVEGDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALRESERVEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BALSHTBOOKVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CAPEXP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CLOSERESERVE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEMENT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPENEXPLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OPENFYDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPENREPLCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPENRESERVE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPENRESIDLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPENWDV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REMLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESERVEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALREPLCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RUNNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VALKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VALUETODISPOSE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WDV), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null