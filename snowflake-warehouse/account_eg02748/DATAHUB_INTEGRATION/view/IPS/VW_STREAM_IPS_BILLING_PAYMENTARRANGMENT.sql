CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYMENTARRANGMENT AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALLOWWARNED::varchar AS ALLOWWARNED, 
                        src:ARRANGEDAMOUNTLTD::numeric(38, 10) AS ARRANGEDAMOUNTLTD, 
                        src:ARRANGEDBY::varchar AS ARRANGEDBY, 
                        src:ARRANGEDDTTM::datetime AS ARRANGEDDTTM, 
                        src:ARRANGEDPAYMENTAMT::numeric(38, 10) AS ARRANGEDPAYMENTAMT, 
                        src:ARRANGEDTOTALAMT::numeric(38, 10) AS ARRANGEDTOTALAMT, 
                        src:ARRANGEDVSBILLEDPERCENT::integer AS ARRANGEDVSBILLEDPERCENT, 
                        src:ARRANGEMENTBALANCE::numeric(38, 10) AS ARRANGEMENTBALANCE, 
                        src:ARRANGEMENTCATEGORY::varchar AS ARRANGEMENTCATEGORY, 
                        src:ARRANGEMENTSTATUS::varchar AS ARRANGEMENTSTATUS, 
                        src:ARRANGEMENTTEMPLATEKEY::integer AS ARRANGEMENTTEMPLATEKEY, 
                        src:ARRANGEMENTTYPE::varchar AS ARRANGEMENTTYPE, 
                        src:BILLEDAMOUNTLTD::numeric(38, 10) AS BILLEDAMOUNTLTD, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENTBALANCE::numeric(38, 10) AS DELINQUENTBALANCE, 
                        src:DOWNPAYMENTAMT::numeric(38, 10) AS DOWNPAYMENTAMT, 
                        src:DOWNPAYMENTDUEDTTM::datetime AS DOWNPAYMENTDUEDTTM, 
                        src:ENDDTTM::datetime AS ENDDTTM, 
                        src:FREQUENCY::varchar AS FREQUENCY, 
                        src:GUARANTORKEY::integer AS GUARANTORKEY, 
                        src:INCLUDECURRENTFLAG::varchar AS INCLUDECURRENTFLAG, 
                        src:INCLUDEDEPOSITFLAG::varchar AS INCLUDEDEPOSITFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFPAYMENTS::integer AS NUMBEROFPAYMENTS, 
                        src:PARENTARRANGEMENTKEY::integer AS PARENTARRANGEMENTKEY, 
                        src:PAYMENTARRANGMENTKEY::integer AS PAYMENTARRANGMENTKEY, 
                        src:PAYMENTSAVERAGEDFLAG::varchar AS PAYMENTSAVERAGEDFLAG, 
                        src:PAYMTSONLYEXCEPTFLAG::varchar AS PAYMTSONLYEXCEPTFLAG, 
                        src:PAYOLDESTFIRSTFLAG::varchar AS PAYOLDESTFIRSTFLAG, 
                        src:SENDNOTICESFLAG::varchar AS SENDNOTICESFLAG, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STATUSDTTM::datetime AS STATUSDTTM, 
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
                                        
                src:PAYMENTARRANGMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYMENTARRANGMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARRANGEDAMOUNTLTD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ARRANGEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARRANGEDPAYMENTAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARRANGEDTOTALAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARRANGEDVSBILLEDPERCENT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARRANGEMENTBALANCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLEDAMOUNTLTD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENTBALANCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DOWNPAYMENTAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DOWNPAYMENTDUEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GUARANTORKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFPAYMENTS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTARRANGEMENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTARRANGMENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null