CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_CASHREGISTER AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CASHSETUP::integer AS CASHSETUP, 
                        src:CHECKSETUP::integer AS CHECKSETUP, 
                        src:CPUSERIAL::varchar AS CPUSERIAL, 
                        src:CREDITSETUP::integer AS CREDITSETUP, 
                        src:DEBITSETUP::integer AS DEBITSETUP, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:ESCROWSETUP::integer AS ESCROWSETUP, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:ISENDORSECHECK::varchar AS ISENDORSECHECK, 
                        src:ISPRINTSIGNATURESLIP::varchar AS ISPRINTSIGNATURESLIP, 
                        src:ISSHOWRECEIPTAPPLET::varchar AS ISSHOWRECEIPTAPPLET, 
                        src:ISUSINGREGISTERGROUP::varchar AS ISUSINGREGISTERGROUP, 
                        src:LOC::varchar AS LOC, 
                        src:MACHINENAME::varchar AS MACHINENAME, 
                        src:MACID::varchar AS MACID, 
                        src:MISCSETUP::integer AS MISCSETUP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYMENTTERMINALFORMULAKEY::integer AS PAYMENTTERMINALFORMULAKEY, 
                        src:RECEIPTCOPIES::integer AS RECEIPTCOPIES, 
                        src:REGID::varchar AS REGID, 
                        src:REGISTERGROUPSETUPKEY::integer AS REGISTERGROUPSETUPKEY, 
                        src:REGKEY::integer AS REGKEY, 
                        src:REGTYPE::varchar AS REGTYPE, 
                        src:SCANNERFORMULAKEY::integer AS SCANNERFORMULAKEY, 
                        src:SUPERVISOR::varchar AS SUPERVISOR, 
                        src:VALIDATEMACHINENAME::varchar AS VALIDATEMACHINENAME, 
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
    
                        
                src:REGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:REGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_CASHREGISTER as strm))