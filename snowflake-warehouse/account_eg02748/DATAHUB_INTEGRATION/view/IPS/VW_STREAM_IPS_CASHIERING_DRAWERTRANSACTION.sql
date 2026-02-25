CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_DRAWERTRANSACTION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDBYIPADDR::varchar AS ADDBYIPADDR, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSLINE1::varchar AS ADDRESSLINE1, 
                        src:ADDRESSLINE2::varchar AS ADDRESSLINE2, 
                        src:ADJREAS::varchar AS ADJREAS, 
                        src:ADJTYPE::varchar AS ADJTYPE, 
                        src:AUTH::varchar AS AUTH, 
                        src:BGTNO::integer AS BGTNO, 
                        src:CARDAUTH::varchar AS CARDAUTH, 
                        src:CARDCVV::varchar AS CARDCVV, 
                        src:CARDEXP::datetime AS CARDEXP, 
                        src:CARDNAME::varchar AS CARDNAME, 
                        src:CARDNUMBER::varchar AS CARDNUMBER, 
                        src:CARDTRACK1::varchar AS CARDTRACK1, 
                        src:CARDTRACK2::varchar AS CARDTRACK2, 
                        src:CARDTRACK3::varchar AS CARDTRACK3, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:CASHIER::varchar AS CASHIER, 
                        src:CHECKACCNO::varchar AS CHECKACCNO, 
                        src:CHECKBANK::varchar AS CHECKBANK, 
                        src:CHECKID::varchar AS CHECKID, 
                        src:CHECKNAME::varchar AS CHECKNAME, 
                        src:CHECKNO::varchar AS CHECKNO, 
                        src:CHECKROUTINGNO::varchar AS CHECKROUTINGNO, 
                        src:CITY::varchar AS CITY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DRWRKEY::integer AS DRWRKEY, 
                        src:DRWRTRANNO::integer AS DRWRTRANNO, 
                        src:ESCROWACCTKEY::integer AS ESCROWACCTKEY, 
                        src:FIRSTNAME::varchar AS FIRSTNAME, 
                        src:LASTNAME::varchar AS LASTNAME, 
                        src:MISCISSUED::varchar AS MISCISSUED, 
                        src:MISCREFNO::varchar AS MISCREFNO, 
                        src:MISCTYPE::varchar AS MISCTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYFLAG::varchar AS PAYFLAG, 
                        src:PAYSERVERTRAN::integer AS PAYSERVERTRAN, 
                        src:PORTALMEMO::varchar AS PORTALMEMO, 
                        src:POSTALCODE::varchar AS POSTALCODE, 
                        src:REFTRANNO::integer AS REFTRANNO, 
                        src:REGKEY::integer AS REGKEY, 
                        src:REGTRANNO::integer AS REGTRANNO, 
                        src:STATE::varchar AS STATE, 
                        src:TENDER::varchar AS TENDER, 
                        src:TRANAMT::numeric(38, 10) AS TRANAMT, 
                        src:TRANBY::varchar AS TRANBY, 
                        src:TRANDTTM::datetime AS TRANDTTM, 
                        src:TRANTYPE::varchar AS TRANTYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOIDDTTM::datetime AS VOIDDTTM, 
                        src:VOIDEDBY::varchar AS VOIDEDBY, 
                        src:VOIDREAS::varchar AS VOIDREAS, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:DRWRTRANNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_DRAWERTRANSACTION as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CARDEXP), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DRWRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DRWRTRANNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWACCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYSERVERTRAN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REFTRANNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRANAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRANDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VOIDDTTM), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null