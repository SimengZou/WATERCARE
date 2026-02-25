CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCOMPLAINTS_COMPLAINTDETAILS AS SELECT
                        src:ACCESSPROBS::varchar AS ACCESSPROBS, 
                        src:ACCTSETUP::varchar AS ACCTSETUP, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLINFO::varchar AS BILLINFO, 
                        src:CALCULATION::varchar AS CALCULATION, 
                        src:CHANGEOWN::varchar AS CHANGEOWN, 
                        src:COMMS::varchar AS COMMS, 
                        src:COMPLAINTDETAILSKEY::integer AS COMPLAINTDETAILSKEY, 
                        src:CUSTDATA::varchar AS CUSTDATA, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISPUTE::varchar AS DISPUTE, 
                        src:ESTIMATE::varchar AS ESTIMATE, 
                        src:FLDSVC::varchar AS FLDSVC, 
                        src:HARDSHIP::varchar AS HARDSHIP, 
                        src:MISREAD::varchar AS MISREAD, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ODUEACCT::varchar AS ODUEACCT, 
                        src:PRICE::varchar AS PRICE, 
                        src:PROCFAIL::varchar AS PROCFAIL, 
                        src:READER::varchar AS READER, 
                        src:READING::varchar AS READING, 
                        src:REFUND::varchar AS REFUND, 
                        src:SERVNO::integer AS SERVNO, 
                        src:SPECFINAL::varchar AS SPECFINAL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WW::varchar AS WW, 
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
    
                        
                src:COMPLAINTDETAILSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:COMPLAINTDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCOMPLAINTS_COMPLAINTDETAILS as strm))