CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGREVIEWSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CNCLREREADRESCODE::varchar AS CNCLREREADRESCODE, 
                        src:DAYSFORCURRD::integer AS DAYSFORCURRD, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESTBKG::integer AS ESTBKG, 
                        src:ESTTXT::integer AS ESTTXT, 
                        src:HI1BKG::integer AS HI1BKG, 
                        src:HI1DEFACTION::integer AS HI1DEFACTION, 
                        src:HI1PCT::integer AS HI1PCT, 
                        src:HI1PROCESS::integer AS HI1PROCESS, 
                        src:HI1TXT::integer AS HI1TXT, 
                        src:HI2BKG::integer AS HI2BKG, 
                        src:HI2DEFACTION::integer AS HI2DEFACTION, 
                        src:HI2PCT::integer AS HI2PCT, 
                        src:HI2PROCESS::integer AS HI2PROCESS, 
                        src:HI2TXT::integer AS HI2TXT, 
                        src:HI3BKG::integer AS HI3BKG, 
                        src:HI3DEFACTION::integer AS HI3DEFACTION, 
                        src:HI3PCT::integer AS HI3PCT, 
                        src:HI3PROCESS::integer AS HI3PROCESS, 
                        src:HI3TXT::integer AS HI3TXT, 
                        src:LO1BKG::integer AS LO1BKG, 
                        src:LO1DEFACTION::integer AS LO1DEFACTION, 
                        src:LO1PCT::integer AS LO1PCT, 
                        src:LO1PROCESS::integer AS LO1PROCESS, 
                        src:LO1TXT::integer AS LO1TXT, 
                        src:LO2BKG::integer AS LO2BKG, 
                        src:LO2DEFACTION::integer AS LO2DEFACTION, 
                        src:LO2PCT::integer AS LO2PCT, 
                        src:LO2PROCESS::integer AS LO2PROCESS, 
                        src:LO2TXT::integer AS LO2TXT, 
                        src:LO3BKG::integer AS LO3BKG, 
                        src:LO3DEFACTION::integer AS LO3DEFACTION, 
                        src:LO3PCT::integer AS LO3PCT, 
                        src:LO3PROCESS::integer AS LO3PROCESS, 
                        src:LO3TXT::integer AS LO3TXT, 
                        src:MARKREREAD::varchar AS MARKREREAD, 
                        src:METERTYPE::varchar AS METERTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEGUSGBKG::integer AS NEGUSGBKG, 
                        src:NEGUSGDEFACTION::integer AS NEGUSGDEFACTION, 
                        src:NEGUSGPROCESS::integer AS NEGUSGPROCESS, 
                        src:NEGUSGTXT::integer AS NEGUSGTXT, 
                        src:NOACCTBKG::integer AS NOACCTBKG, 
                        src:NOACCTDEFACTION::integer AS NOACCTDEFACTION, 
                        src:NOACCTPROCESS::integer AS NOACCTPROCESS, 
                        src:NOACCTTXT::integer AS NOACCTTXT, 
                        src:NORDBKG::integer AS NORDBKG, 
                        src:NORDDEFACTION::integer AS NORDDEFACTION, 
                        src:NORMALPROCESS::integer AS NORMALPROCESS, 
                        src:NOUSGBKG::integer AS NOUSGBKG, 
                        src:NOUSGDEFACTION::integer AS NOUSGDEFACTION, 
                        src:NOUSGPROCESS::integer AS NOUSGPROCESS, 
                        src:NOUSGTXT::integer AS NOUSGTXT, 
                        src:PRESELECTACCTFORBILL::varchar AS PRESELECTACCTFORBILL, 
                        src:RDPROBKEY::integer AS RDPROBKEY, 
                        src:RDREVIEWSUKEY::integer AS RDREVIEWSUKEY, 
                        src:UPONADDIREADAPPROVAL::integer AS UPONADDIREADAPPROVAL, 
                        src:UPONCYREADAPPROVAL::integer AS UPONCYREADAPPROVAL, 
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
                                        
                src:RDREVIEWSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGREVIEWSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAYSFORCURRD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESTBKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESTTXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI1BKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI1DEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI1PCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI1PROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI1TXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI2BKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI2DEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI2PCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI2PROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI2TXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI3BKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI3DEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI3PCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI3PROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HI3TXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO1BKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO1DEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO1PCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO1PROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO1TXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO2BKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO2DEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO2PCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO2PROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO2TXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO3BKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO3DEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO3PCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO3PROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LO3TXT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEGUSGBKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEGUSGDEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEGUSGPROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEGUSGTXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOACCTBKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOACCTDEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOACCTPROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOACCTTXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NORDBKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NORDDEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NORMALPROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOUSGBKG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOUSGDEFACTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOUSGPROCESS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOUSGTXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RDPROBKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RDREVIEWSUKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPONADDIREADAPPROVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPONCYREADAPPROVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null