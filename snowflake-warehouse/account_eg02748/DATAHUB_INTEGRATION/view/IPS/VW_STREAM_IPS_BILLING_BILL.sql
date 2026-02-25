CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILL AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCTBALCALCFLAG::varchar AS ACCTBALCALCFLAG, 
                        src:ACTUALAMT::numeric(38, 10) AS ACTUALAMT, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDR1::varchar AS ADDR1, 
                        src:ADDR2::varchar AS ADDR2, 
                        src:ADJAMT::numeric(38, 10) AS ADJAMT, 
                        src:ADJUSTMENTLINEITEMSAMOUNT::numeric(38, 10) AS ADJUSTMENTLINEITEMSAMOUNT, 
                        src:ARNGAMT::numeric(38, 10) AS ARNGAMT, 
                        src:ARNGPAYAMT::numeric(38, 10) AS ARNGPAYAMT, 
                        src:BATCHIDENTIFIER::integer AS BATCHIDENTIFIER, 
                        src:BILLADJAMT::numeric(38, 10) AS BILLADJAMT, 
                        src:BILLAMT::numeric(38, 10) AS BILLAMT, 
                        src:BILLDATE::datetime AS BILLDATE, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BILLNO::varchar AS BILLNO, 
                        src:BILLPERFRDATE::datetime AS BILLPERFRDATE, 
                        src:BILLPERTODATE::datetime AS BILLPERTODATE, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:BILLTO::varchar AS BILLTO, 
                        src:BILLTOLINE2::varchar AS BILLTOLINE2, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:BUDGETBILLINGPLANKEY::integer AS BUDGETBILLINGPLANKEY, 
                        src:CALCULATEDFLAG::varchar AS CALCULATEDFLAG, 
                        src:CARRT::varchar AS CARRT, 
                        src:CASSVER::integer AS CASSVER, 
                        src:CITY::varchar AS CITY, 
                        src:COLLAMT::numeric(38, 10) AS COLLAMT, 
                        src:COLLKEY::integer AS COLLKEY, 
                        src:CONAME::varchar AS CONAME, 
                        src:CONCCRED::numeric(38, 10) AS CONCCRED, 
                        src:CONCSAPPLFLAG::varchar AS CONCSAPPLFLAG, 
                        src:CORRKEY::integer AS CORRKEY, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:CREATEDDTTM::datetime AS CREATEDDTTM, 
                        src:CREDITFLAG::varchar AS CREDITFLAG, 
                        src:CUMVARIAMT::numeric(38, 10) AS CUMVARIAMT, 
                        src:CURRAMT::numeric(38, 10) AS CURRAMT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DAYSRDS::integer AS DAYSRDS, 
                        src:DEFERREDPENALTYAMOUNT::numeric(38, 10) AS DEFERREDPENALTYAMOUNT, 
                        src:DEFERREDPENALTYDATE::datetime AS DEFERREDPENALTYDATE, 
                        src:DELINQUENTDATE::datetime AS DELINQUENTDATE, 
                        src:DEPAMT::numeric(38, 10) AS DEPAMT, 
                        src:DEPDUEAMT::numeric(38, 10) AS DEPDUEAMT, 
                        src:DEPOSITLINEITEMSAMOUNT::numeric(38, 10) AS DEPOSITLINEITEMSAMOUNT, 
                        src:DIRECTDEBITRUNKEY::integer AS DIRECTDEBITRUNKEY, 
                        src:DISCEXPAMT::numeric(38, 10) AS DISCEXPAMT, 
                        src:DISCEXPDATE::datetime AS DISCEXPDATE, 
                        src:DISCOUNTLINEITEMSAMOUNT::numeric(38, 10) AS DISCOUNTLINEITEMSAMOUNT, 
                        src:DISPAMT::numeric(38, 10) AS DISPAMT, 
                        src:DLNQAMT::numeric(38, 10) AS DLNQAMT, 
                        src:DPC::varchar AS DPC, 
                        src:DUEDATE::datetime AS DUEDATE, 
                        src:EARLYPAYORDEFERREDPENALTY::varchar AS EARLYPAYORDEFERREDPENALTY, 
                        src:ENTNO::varchar AS ENTNO, 
                        src:ESTIMATEFLAG::varchar AS ESTIMATEFLAG, 
                        src:EXCLUSIVEFLAG::varchar AS EXCLUSIVEFLAG, 
                        src:EXTRACTDATE::datetime AS EXTRACTDATE, 
                        src:FINALBILLFLAG::varchar AS FINALBILLFLAG, 
                        src:INSTNO::integer AS INSTNO, 
                        src:INSTTOTAMT::numeric(38, 10) AS INSTTOTAMT, 
                        src:INTAMT::numeric(38, 10) AS INTAMT, 
                        src:INTERIMFLAG::varchar AS INTERIMFLAG, 
                        src:LIENAMT::numeric(38, 10) AS LIENAMT, 
                        src:LIENKEY::integer AS LIENKEY, 
                        src:LOT::varchar AS LOT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOBILLFLAG::varchar AS NOBILLFLAG, 
                        src:OUTPUTFLAG::varchar AS OUTPUTFLAG, 
                        src:OVERAMT::numeric(38, 10) AS OVERAMT, 
                        src:PAIDDTTM::datetime AS PAIDDTTM, 
                        src:PAIDSTAT::varchar AS PAIDSTAT, 
                        src:PAYMENTLINEITEMSAMOUNT::numeric(38, 10) AS PAYMENTLINEITEMSAMOUNT, 
                        src:PAYORDER::integer AS PAYORDER, 
                        src:PENALTYLINEITEMSAMOUNT::numeric(38, 10) AS PENALTYLINEITEMSAMOUNT, 
                        src:PENAMT::numeric(38, 10) AS PENAMT, 
                        src:PENEXEMPT::varchar AS PENEXEMPT, 
                        src:POSTEDFLAG::varchar AS POSTEDFLAG, 
                        src:POSTEDTTM::datetime AS POSTEDTTM, 
                        src:PREVIOUSBILLKEY::integer AS PREVIOUSBILLKEY, 
                        src:PRINCIPALTOTALAMOUNT::numeric(38, 10) AS PRINCIPALTOTALAMOUNT, 
                        src:PRIORBILLAMT::numeric(38, 10) AS PRIORBILLAMT, 
                        src:PROPADDR::varchar AS PROPADDR, 
                        src:REVERSEDBILLKEY::integer AS REVERSEDBILLKEY, 
                        src:REVERSEFLAG::varchar AS REVERSEFLAG, 
                        src:SETLAMT::numeric(38, 10) AS SETLAMT, 
                        src:SETTLEMENTKEY::integer AS SETTLEMENTKEY, 
                        src:STATE::varchar AS STATE, 
                        src:VARIAMT::numeric(38, 10) AS VARIAMT, 
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
                                        
                src:BILLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTUALAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADJUSTMENTLINEITEMSAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARNGAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ARNGPAYAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHIDENTIFIER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLADJAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLPERFRDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLPERTODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETBILLINGPLANKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COLLAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COLLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONCCRED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CORRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CUMVARIAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAYSRDS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEFERREDPENALTYAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DEFERREDPENALTYDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DELINQUENTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPDUEAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPOSITLINEITEMSAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCEXPAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISCEXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCOUNTLINEITEMSAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DLNQAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSTNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSTTOTAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INTAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LIENAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LIENKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OVERAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PAIDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTLINEITEMSAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PENALTYLINEITEMSAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PENAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:POSTEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PREVIOUSBILLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRINCIPALTOTALAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRIORBILLAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSEDBILLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SETLAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SETTLEMENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null