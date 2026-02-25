CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCOUNTTRADEWASTE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ACCOUNTTRADEWASTEKEY::integer AS ACCOUNTTRADEWASTEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPEARANCE::varchar AS APPEARANCE, 
                        src:AREA::varchar AS AREA, 
                        src:BILLEDFLAG::varchar AS BILLEDFLAG, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:COMMENTKEY::integer AS COMMENTKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DOCKETNUMBER::varchar AS DOCKETNUMBER, 
                        src:EMPLOYEEID::varchar AS EMPLOYEEID, 
                        src:FACILITY::varchar AS FACILITY, 
                        src:GROSS::numeric(38, 10) AS GROSS, 
                        src:INDTTM::datetime AS INDTTM, 
                        src:LATEFEEAMOUNT::numeric(38, 10) AS LATEFEEAMOUNT, 
                        src:LATEFEEFLAG::varchar AS LATEFEEFLAG, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:LOCATION::varchar AS LOCATION, 
                        src:MANIFESTFLAG::varchar AS MANIFESTFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NET::numeric(38, 10) AS NET, 
                        src:NOMINATEDONEOFF::integer AS NOMINATEDONEOFF, 
                        src:ODOR::varchar AS ODOR, 
                        src:ORIGIN::varchar AS ORIGIN, 
                        src:OUTDTTM::datetime AS OUTDTTM, 
                        src:PURCHASENUMBER::varchar AS PURCHASENUMBER, 
                        src:TARE::numeric(38, 10) AS TARE, 
                        src:TRAILERREGKEY::integer AS TRAILERREGKEY, 
                        src:TWVEHICLEREGKEY::integer AS TWVEHICLEREGKEY, 
                        src:UNIT::integer AS UNIT, 
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
                                        
                src:ACCOUNTTRADEWASTEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTTRADEWASTE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTTRADEWASTEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GROSS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LATEFEEAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOMINATEDONEOFF), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OUTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TARE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRAILERREGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TWVEHICLEREGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UNIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null