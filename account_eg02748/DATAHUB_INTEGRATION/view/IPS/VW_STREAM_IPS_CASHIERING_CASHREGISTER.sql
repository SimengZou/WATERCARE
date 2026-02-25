CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_CASHREGISTER AS SELECT
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
                                        
                src:REGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_CASHREGISTER as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CASHSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CHECKSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CREDITSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEBITSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MISCSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTTERMINALFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RECEIPTCOPIES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REGISTERGROUPSETUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCANNERFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null