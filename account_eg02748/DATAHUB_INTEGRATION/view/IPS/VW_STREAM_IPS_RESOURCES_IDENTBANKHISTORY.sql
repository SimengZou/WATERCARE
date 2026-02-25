CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_IDENTBANKHISTORY AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BANK::varchar AS BANK, 
                        src:BANKACCOUNTNUMBER::varchar AS BANKACCOUNTNUMBER, 
                        src:BANKACCOUNTTYPE::varchar AS BANKACCOUNTTYPE, 
                        src:BANKADDRESS::varchar AS BANKADDRESS, 
                        src:BANKBRANCH::varchar AS BANKBRANCH, 
                        src:BANKCITY::varchar AS BANKCITY, 
                        src:BANKPHONE::varchar AS BANKPHONE, 
                        src:BANKROUTINGNUMBER::varchar AS BANKROUTINGNUMBER, 
                        src:BANKSTATE::varchar AS BANKSTATE, 
                        src:BANKZIP::varchar AS BANKZIP, 
                        src:CARDEXPIREDATE::datetime AS CARDEXPIREDATE, 
                        src:CARDNUMBER::varchar AS CARDNUMBER, 
                        src:CARDSECURITYNUMBER::varchar AS CARDSECURITYNUMBER, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIRECTDEBITSTATUS::varchar AS DIRECTDEBITSTATUS, 
                        src:DIRECTDEBITTRANSMITDATE::datetime AS DIRECTDEBITTRANSMITDATE, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:IDENTBANKHISTORYKEY::integer AS IDENTBANKHISTORYKEY, 
                        src:IDENTITYKEY::integer AS IDENTITYKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NAMEONACCOUNT::varchar AS NAMEONACCOUNT, 
                        src:PRENOTEDDRUNKEY::integer AS PRENOTEDDRUNKEY, 
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
                                        
                src:IDENTBANKHISTORYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_IDENTBANKHISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CARDEXPIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DIRECTDEBITTRANSMITDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IDENTBANKHISTORYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IDENTITYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRENOTEDDRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null