CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_JOURNAL AS SELECT
                        src:ACCOUNTINGPERIOD::integer AS ACCOUNTINGPERIOD, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:CREDITBUDGETNUMBERKEY::integer AS CREDITBUDGETNUMBERKEY, 
                        src:DEBITBUDGETNUMBERKEY::integer AS DEBITBUDGETNUMBERKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:FISCALYEAR::integer AS FISCALYEAR, 
                        src:JOURNALKEY::integer AS JOURNALKEY, 
                        src:JOURNALRUNKEY::integer AS JOURNALRUNKEY, 
                        src:JOURNALSUMMARYKEY::integer AS JOURNALSUMMARYKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTIDISTRIBUTIOLNALFLAG::varchar AS MULTIDISTRIBUTIOLNALFLAG, 
                        src:ORIGIN::varchar AS ORIGIN, 
                        src:TAXINCLUSIVEFLAG::varchar AS TAXINCLUSIVEFLAG, 
                        src:TRANSACTIONBY::varchar AS TRANSACTIONBY, 
                        src:TRANSACTIONDTTM::datetime AS TRANSACTIONDTTM, 
                        src:TRANSACTIONNUMBER::integer AS TRANSACTIONNUMBER, 
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
                                        
                src:JOURNALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_JOURNAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTINGPERIOD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CREDITBUDGETNUMBERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEBITBUDGETNUMBERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FISCALYEAR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:JOURNALKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:JOURNALRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:JOURNALSUMMARYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRANSACTIONDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRANSACTIONNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null