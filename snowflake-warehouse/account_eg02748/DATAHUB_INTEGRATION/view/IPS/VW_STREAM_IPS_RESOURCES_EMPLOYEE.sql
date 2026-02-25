CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_EMPLOYEE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BNFTRATE::numeric(38, 10) AS BNFTRATE, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:CONTACTKEY::integer AS CONTACTKEY, 
                        src:CREWLDR::varchar AS CREWLDR, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEPT::varchar AS DEPT, 
                        src:EMERGINFO::varchar AS EMERGINFO, 
                        src:EMERGNAME1::varchar AS EMERGNAME1, 
                        src:EMERGNAME2::varchar AS EMERGNAME2, 
                        src:EMERGNAME3::varchar AS EMERGNAME3, 
                        src:EMERGPHN1::varchar AS EMERGPHN1, 
                        src:EMERGPHN2::varchar AS EMERGPHN2, 
                        src:EMERGPHN3::varchar AS EMERGPHN3, 
                        src:EMERGREL1::varchar AS EMERGREL1, 
                        src:EMERGREL2::varchar AS EMERGREL2, 
                        src:EMERGREL3::varchar AS EMERGREL3, 
                        src:EMPID::varchar AS EMPID, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:HIREDATE::datetime AS HIREDATE, 
                        src:INSPFLAG::varchar AS INSPFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PINAUTHCODE::varchar AS PINAUTHCODE, 
                        src:RATE::numeric(38, 10) AS RATE, 
                        src:SECT::varchar AS SECT, 
                        src:SUPR::varchar AS SUPR, 
                        src:SUPRFLAG::varchar AS SUPRFLAG, 
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
                                        
                src:EMPID  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_EMPLOYEE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BNFTRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:HIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null