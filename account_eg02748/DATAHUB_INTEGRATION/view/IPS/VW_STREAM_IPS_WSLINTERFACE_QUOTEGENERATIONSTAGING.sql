CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APDTTM::datetime AS APDTTM, 
                        src:APNO::varchar AS APNO, 
                        src:CUSTOMERNAME::varchar AS CUSTOMERNAME, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DUEDAYS::integer AS DUEDAYS, 
                        src:FEEPAYERNAME::varchar AS FEEPAYERNAME, 
                        src:FROMEMAIL::varchar AS FROMEMAIL, 
                        src:GST::numeric(38, 10) AS GST, 
                        src:KEYCOLUMN::integer AS KEYCOLUMN, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MONIKER::varchar AS MONIKER, 
                        src:NAME::varchar AS NAME, 
                        src:OCCUPANCYTYPE::varchar AS OCCUPANCYTYPE, 
                        src:QUOTEACCEPTANCEEMAIL::varchar AS QUOTEACCEPTANCEEMAIL, 
                        src:QUOTEGENERATIONSTAGINGKEY::integer AS QUOTEGENERATIONSTAGINGKEY, 
                        src:QUOTETYPE::varchar AS QUOTETYPE, 
                        src:REVIEWERDDI::varchar AS REVIEWERDDI, 
                        src:REVIEWEREMAIL::varchar AS REVIEWEREMAIL, 
                        src:REVIEWERNAME::varchar AS REVIEWERNAME, 
                        src:REVIEWERROLE::varchar AS REVIEWERROLE, 
                        src:SOURCE::varchar AS SOURCE, 
                        src:STREETNUMBER::varchar AS STREETNUMBER, 
                        src:SUBJECT::varchar AS SUBJECT, 
                        src:TITLE::varchar AS TITLE, 
                        src:TOEMAIL::varchar AS TOEMAIL, 
                        src:TOTALAFTERTAX::numeric(38, 10) AS TOTALAFTERTAX, 
                        src:TOTALVALUE::numeric(38, 10) AS TOTALVALUE, 
                        src:VALIDTILL::datetime AS VALIDTILL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERCAREREFNO::varchar AS WATERCAREREFNO, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:QUOTEGENERATIONSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DUEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KEYCOLUMN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUOTEGENERATIONSTAGINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALAFTERTAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VALIDTILL), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null