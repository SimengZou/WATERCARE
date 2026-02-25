CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_BUILDING_BLDGASSOCPROP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:APBLDGASSOCPROPKEY::integer AS APBLDGASSOCPROPKEY, 
                        src:APBLDGKEY::integer AS APBLDGKEY, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENDDTTM::datetime AS ENDDTTM, 
                        src:GROUPKEY::integer AS GROUPKEY, 
                        src:LINEARFROMFT::numeric(38, 10) AS LINEARFROMFT, 
                        src:LINEARFROMM::numeric(38, 10) AS LINEARFROMM, 
                        src:LINEARTOFT::numeric(38, 10) AS LINEARTOFT, 
                        src:LINEARTOM::numeric(38, 10) AS LINEARTOM, 
                        src:LINKEDLOCFLAG::varchar AS LINKEDLOCFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PROPKEY::integer AS PROPKEY, 
                        src:PROPOCCUPKEY::integer AS PROPOCCUPKEY, 
                        src:SITERELATIONSHIP::varchar AS SITERELATIONSHIP, 
                        src:SITESTATUS::varchar AS SITESTATUS, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:TYPE::integer AS TYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:APBLDGASSOCPROPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGASSOCPROP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGASSOCPROPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPOCCUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null