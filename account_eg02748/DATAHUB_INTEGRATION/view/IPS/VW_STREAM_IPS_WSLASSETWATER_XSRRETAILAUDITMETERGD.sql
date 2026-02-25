CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETWATER_XSRRETAILAUDITMETERGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEACTIVATE::varchar AS DEACTIVATE, 
                        src:FREQUENCY::varchar AS FREQUENCY, 
                        src:HOUSENUMBER::varchar AS HOUSENUMBER, 
                        src:INSTALLEDDATE::datetime AS INSTALLEDDATE, 
                        src:METERID::varchar AS METERID, 
                        src:METERLOCATION::varchar AS METERLOCATION, 
                        src:METERTYPE::varchar AS METERTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFDIALS::integer AS NUMBEROFDIALS, 
                        src:OLDMETERID::varchar AS OLDMETERID, 
                        src:PHONENUMBER::varchar AS PHONENUMBER, 
                        src:REMOVEDDATE::datetime AS REMOVEDDATE, 
                        src:ROUTEID::varchar AS ROUTEID, 
                        src:SERVNO::integer AS SERVNO, 
                        src:SMS::varchar AS SMS, 
                        src:STREETNAME::varchar AS STREETNAME, 
                        src:STREETTYPE::varchar AS STREETTYPE, 
                        src:SUBURB::varchar AS SUBURB, 
                        src:UNITNUMBER::varchar AS UNITNUMBER, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XSRRETAILAUDITMETERDPKEY::integer AS XSRRETAILAUDITMETERDPKEY, 
                        src:XSRRETAILAUDITMETERGDKEY::integer AS XSRRETAILAUDITMETERGDKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XSRRETAILAUDITMETERGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_XSRRETAILAUDITMETERGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSTALLEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFDIALS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REMOVEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XSRRETAILAUDITMETERDPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XSRRETAILAUDITMETERGDKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null