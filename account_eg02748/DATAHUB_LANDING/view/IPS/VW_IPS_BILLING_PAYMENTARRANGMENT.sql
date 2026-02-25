CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_PAYMENTARRANGMENT AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:PAYMENTARRANGMENTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PAYMENTARRANGMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_PAYMENTARRANGMENT as strm))