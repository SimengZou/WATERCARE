CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLTRADEBILLOUTPUT_TRADEBILLOUTPUTEXT AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ACCTCLASS::varchar AS ACCTCLASS, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BARCODEAMOUNT::varchar AS BARCODEAMOUNT, 
                        src:BARCODECHECKDIGIT::varchar AS BARCODECHECKDIGIT, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:CHECKDIGIT::varchar AS CHECKDIGIT, 
                        src:CUSTOMERREF::varchar AS CUSTOMERREF, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DDFLAG::varchar AS DDFLAG, 
                        src:DTLRCT::datetime AS DTLRCT, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:MICRCHECKDIGIT::varchar AS MICRCHECKDIGIT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PREVBILLAMT::numeric(38, 10) AS PREVBILLAMT, 
                        src:TRADEBILLOUTPUTEXTKEY::integer AS TRADEBILLOUTPUTEXTKEY, 
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
    
                        
                src:TRADEBILLOUTPUTEXTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TRADEBILLOUTPUTEXTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLTRADEBILLOUTPUT_TRADEBILLOUTPUTEXT as strm))