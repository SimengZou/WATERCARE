CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD AS SELECT
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
                        src:WDV::numeric(38, 10) AS WDV, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
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
    
                        
                src:ASSVALRESERVEGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASSVALRESERVEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD as strm))