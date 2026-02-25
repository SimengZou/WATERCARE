CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_LINEITEMRATE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSERVICEKEY::integer AS ADDRESSSERVICEKEY, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:CALCULATIONDETAILKEY::integer AS CALCULATIONDETAILKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPOSITCHARGEKEY::integer AS DEPOSITCHARGEKEY, 
                        src:FIXEDCHGCHARGEDAYS::integer AS FIXEDCHGCHARGEDAYS, 
                        src:FIXEDCHGFROMDATE::datetime AS FIXEDCHGFROMDATE, 
                        src:FIXEDCHGNUMBERPERIODS::integer AS FIXEDCHGNUMBERPERIODS, 
                        src:FIXEDCHGPERIODDAYS::integer AS FIXEDCHGPERIODDAYS, 
                        src:FIXEDCHGPERIODSINYEAR::integer AS FIXEDCHGPERIODSINYEAR, 
                        src:FIXEDCHGRATEPERPERIOD::numeric(38, 10) AS FIXEDCHGRATEPERPERIOD, 
                        src:FIXEDCHGTODATE::datetime AS FIXEDCHGTODATE, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:LINEITEMRATEKEY::integer AS LINEITEMRATEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONEOFFCHARGEKEY::integer AS ONEOFFCHARGEKEY, 
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
                                        
                src:LINEITEMRATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_LINEITEMRATE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CALCULATIONDETAILKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FIXEDCHGCHARGEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FIXEDCHGFROMDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FIXEDCHGNUMBERPERIODS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FIXEDCHGPERIODDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FIXEDCHGPERIODSINYEAR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FIXEDCHGRATEPERPERIOD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FIXEDCHGTODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMRATEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null