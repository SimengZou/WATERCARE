CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_SENDCOPIESTO AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDR1::varchar AS ADDR1, 
                        src:ADDR2::varchar AS ADDR2, 
                        src:ADDRESSCONTACTKEY::integer AS ADDRESSCONTACTKEY, 
                        src:CARRT::varchar AS CARRT, 
                        src:CASSBARCODE::integer AS CASSBARCODE, 
                        src:CASSVER::integer AS CASSVER, 
                        src:CITY::varchar AS CITY, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DPC::varchar AS DPC, 
                        src:FROMDATE::datetime AS FROMDATE, 
                        src:LOT::varchar AS LOT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:SEASADDR1::varchar AS SEASADDR1, 
                        src:SEASADDR2::varchar AS SEASADDR2, 
                        src:SEASCARRT::varchar AS SEASCARRT, 
                        src:SEASCASSBARCODE::integer AS SEASCASSBARCODE, 
                        src:SEASCASSVER::integer AS SEASCASSVER, 
                        src:SEASCITY::varchar AS SEASCITY, 
                        src:SEASCOUNTRY::varchar AS SEASCOUNTRY, 
                        src:SEASDPC::varchar AS SEASDPC, 
                        src:SEASFROMDATE::datetime AS SEASFROMDATE, 
                        src:SEASLOT::varchar AS SEASLOT, 
                        src:SEASSTATE::varchar AS SEASSTATE, 
                        src:SEASTODATE::datetime AS SEASTODATE, 
                        src:SEASZIP::varchar AS SEASZIP, 
                        src:SENDCOPIESQUANTITY::integer AS SENDCOPIESQUANTITY, 
                        src:SENDCOPIESTOKEY::integer AS SENDCOPIESTOKEY, 
                        src:SENDTOLINE1::varchar AS SENDTOLINE1, 
                        src:SENDTOLINE2::varchar AS SENDTOLINE2, 
                        src:STATE::varchar AS STATE, 
                        src:TODATE::datetime AS TODATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:ZIP::varchar AS ZIP, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:SENDCOPIESTOKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_SENDCOPIESTO as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSCONTACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CASSBARCODE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FROMDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEASCASSBARCODE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEASCASSVER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SEASFROMDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SEASTODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SENDCOPIESQUANTITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SENDCOPIESTOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null