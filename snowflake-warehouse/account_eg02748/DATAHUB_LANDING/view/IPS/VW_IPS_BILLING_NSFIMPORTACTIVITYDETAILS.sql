CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_NSFIMPORTACTIVITYDETAILS AS SELECT
                        src:ACCOUNTIDENTIFIER::varchar AS ACCOUNTIDENTIFIER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:AUTOFLAG::varchar AS AUTOFLAG, 
                        src:CASHERINGBASEDFLAG::varchar AS CASHERINGBASEDFLAG, 
                        src:CHECKNUMBER::varchar AS CHECKNUMBER, 
                        src:DATAROW::varchar AS DATAROW, 
                        src:DELETED::boolean AS DELETED, 
                        src:EMPLOYEE::varchar AS EMPLOYEE, 
                        src:EXCEPTIONDESC::varchar AS EXCEPTIONDESC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTES::varchar AS NOTES, 
                        src:NSFCREATEDFLAG::varchar AS NSFCREATEDFLAG, 
                        src:NSFIMPORTACTIVITYDTLKEY::integer AS NSFIMPORTACTIVITYDTLKEY, 
                        src:NSFIMPORTACTIVITYKEY::integer AS NSFIMPORTACTIVITYKEY, 
                        src:NSFPROCESSEXCEPTION::integer AS NSFPROCESSEXCEPTION, 
                        src:PAYMENTBATCHKEY::integer AS PAYMENTBATCHKEY, 
                        src:PAYMENTIDENTIFIER::integer AS PAYMENTIDENTIFIER, 
                        src:RERUNFLAG::varchar AS RERUNFLAG, 
                        src:RESOLVEDFLAG::varchar AS RESOLVEDFLAG, 
                        src:RETURNREASONCODE::varchar AS RETURNREASONCODE, 
                        src:REVERSEDFLAG::varchar AS REVERSEDFLAG, 
                        src:REVIEWDATE::datetime AS REVIEWDATE, 
                        src:REVIEWEDFLAG::varchar AS REVIEWEDFLAG, 
                        src:TRANSACTIONDATE::datetime AS TRANSACTIONDATE, 
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
    
                        
                src:NSFIMPORTACTIVITYDTLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:NSFIMPORTACTIVITYDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_NSFIMPORTACTIVITYDETAILS as strm))