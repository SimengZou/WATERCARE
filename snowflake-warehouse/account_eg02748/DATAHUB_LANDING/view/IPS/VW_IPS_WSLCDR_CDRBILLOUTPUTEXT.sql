CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDR_CDRBILLOUTPUTEXT AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ACCTCLASS::varchar AS ACCTCLASS, 
                        src:ACCTNUM::varchar AS ACCTNUM, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:ADDRESSLINE1::varchar AS ADDRESSLINE1, 
                        src:ADDRESSLINE2::varchar AS ADDRESSLINE2, 
                        src:ADJUSTMENTAMOUNT::numeric(38, 10) AS ADJUSTMENTAMOUNT, 
                        src:APPLICATIONNAME::varchar AS APPLICATIONNAME, 
                        src:APPLICATIONNUMBER::varchar AS APPLICATIONNUMBER, 
                        src:APPLICATIONTYPE::varchar AS APPLICATIONTYPE, 
                        src:BALANCECURRENTCHARGE::numeric(38, 10) AS BALANCECURRENTCHARGE, 
                        src:BARCODEACCOUNTNUMBER::varchar AS BARCODEACCOUNTNUMBER, 
                        src:BARCODEAMOUNT::varchar AS BARCODEAMOUNT, 
                        src:BARCODECHECKDIGIT::varchar AS BARCODECHECKDIGIT, 
                        src:BARCODEDUEDATE::datetime AS BARCODEDUEDATE, 
                        src:BILLAMOUNT::numeric(38, 10) AS BILLAMOUNT, 
                        src:BILLDATE::datetime AS BILLDATE, 
                        src:BILLDELIVERYTYPE::varchar AS BILLDELIVERYTYPE, 
                        src:BILLNUMBER::varchar AS BILLNUMBER, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:BILLTO::varchar AS BILLTO, 
                        src:BILLTYPE::varchar AS BILLTYPE, 
                        src:BRFWRD::numeric(38, 10) AS BRFWRD, 
                        src:CCADDRESS::varchar AS CCADDRESS, 
                        src:CDRBILLOUTPUTEXTKEY::integer AS CDRBILLOUTPUTEXTKEY, 
                        src:CHECKDIGIT::varchar AS CHECKDIGIT, 
                        src:CITY::varchar AS CITY, 
                        src:CONTROLTOTAL::numeric(38, 10) AS CONTROLTOTAL, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:CURRENTBILLKEY::integer AS CURRENTBILLKEY, 
                        src:CURRPENAMT::numeric(38, 10) AS CURRPENAMT, 
                        src:CUSTOMERNO::varchar AS CUSTOMERNO, 
                        src:CUSTOMERREF::varchar AS CUSTOMERREF, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIRECTDEBITFLAG::varchar AS DIRECTDEBITFLAG, 
                        src:DISCAMOUNTDUE::numeric(38, 10) AS DISCAMOUNTDUE, 
                        src:DISCTOTALDUE::numeric(38, 10) AS DISCTOTALDUE, 
                        src:DUEDATE::datetime AS DUEDATE, 
                        src:EBILLTYPE::varchar AS EBILLTYPE, 
                        src:EMAILADDRESS::varchar AS EMAILADDRESS, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:GSTAMOUNT::numeric(38, 10) AS GSTAMOUNT, 
                        src:LASTPAYMENTDATE::datetime AS LASTPAYMENTDATE, 
                        src:LOT::varchar AS LOT, 
                        src:MICRCHECKDIGIT::varchar AS MICRCHECKDIGIT, 
                        src:MICROACCOUNTNUMBER::varchar AS MICROACCOUNTNUMBER, 
                        src:MICROAMOUNT::varchar AS MICROAMOUNT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFINVOICES::integer AS NUMBEROFINVOICES, 
                        src:OPENBALANCE::numeric(38, 10) AS OPENBALANCE, 
                        src:PARENTACCOUNTNUMBER::varchar AS PARENTACCOUNTNUMBER, 
                        src:PARENTACCTKEY::integer AS PARENTACCTKEY, 
                        src:PARENTADDRESS::varchar AS PARENTADDRESS, 
                        src:PARENTAPPLICATIONNUMBER::varchar AS PARENTAPPLICATIONNUMBER, 
                        src:PARENTAPPLICATIONREFERENCE::varchar AS PARENTAPPLICATIONREFERENCE, 
                        src:PARENTCUSTOMERNO::varchar AS PARENTCUSTOMERNO, 
                        src:PAYMENTAMOUNT::numeric(38, 10) AS PAYMENTAMOUNT, 
                        src:POSTALCODE::varchar AS POSTALCODE, 
                        src:PREVBILLAMOUNT::numeric(38, 10) AS PREVBILLAMOUNT, 
                        src:PRINCIPALTOTAL::numeric(38, 10) AS PRINCIPALTOTAL, 
                        src:RUNNUMBER::integer AS RUNNUMBER, 
                        src:STATE::varchar AS STATE, 
                        src:TOADDRESS::varchar AS TOADDRESS, 
                        src:TOTALDUE::numeric(38, 10) AS TOTALDUE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XERO::varchar AS XERO, 
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
    
                        
                src:CDRBILLOUTPUTEXTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CDRBILLOUTPUTEXTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDR_CDRBILLOUTPUTEXT as strm))