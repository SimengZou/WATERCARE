CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_CNTCTID AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CONFIDENTFLAG::varchar AS CONFIDENTFLAG, 
                        src:CONTRACTORID::varchar AS CONTRACTORID, 
                        src:CONTRACTORRATE::numeric(38, 10) AS CONTRACTORRATE, 
                        src:CONTRACTORTYPE::varchar AS CONTRACTORTYPE, 
                        src:CUSTOMERNO::varchar AS CUSTOMERNO, 
                        src:DEATHDATE::datetime AS DEATHDATE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DOB::datetime AS DOB, 
                        src:DRIVERLICENSENO::varchar AS DRIVERLICENSENO, 
                        src:DRIVERLICENSESTATE::varchar AS DRIVERLICENSESTATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:EXTID::varchar AS EXTID, 
                        src:FEDTAXID::varchar AS FEDTAXID, 
                        src:FULLNAME::varchar AS FULLNAME, 
                        src:HINT::varchar AS HINT, 
                        src:IDKEY::integer AS IDKEY, 
                        src:IDLAST4::varchar AS IDLAST4, 
                        src:IDNO::varchar AS IDNO, 
                        src:IDTYPE::varchar AS IDTYPE, 
                        src:ISCONTRACTOR::varchar AS ISCONTRACTOR, 
                        src:LANGUAGE::varchar AS LANGUAGE, 
                        src:MAIDENNAME::varchar AS MAIDENNAME, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NAMEFIRST::varchar AS NAMEFIRST, 
                        src:NAMELAST::varchar AS NAMELAST, 
                        src:NAMEMID::varchar AS NAMEMID, 
                        src:PASSWORD::varchar AS PASSWORD, 
                        src:PIN::integer AS PIN, 
                        src:PRIMARYDUPLICATE::integer AS PRIMARYDUPLICATE, 
                        src:REQUESTEDNAME::varchar AS REQUESTEDNAME, 
                        src:SECURITYANSWER::varchar AS SECURITYANSWER, 
                        src:STTAXID::varchar AS STTAXID, 
                        src:SUFFIX::varchar AS SUFFIX, 
                        src:TITLE::varchar AS TITLE, 
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
                                        
                src:IDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_CNTCTID as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTRACTORRATE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DEATHDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DOB), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRIMARYDUPLICATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null