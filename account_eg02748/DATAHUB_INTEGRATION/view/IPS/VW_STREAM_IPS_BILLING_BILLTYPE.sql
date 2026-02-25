CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILLTYPE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLYCREDITTYPE::integer AS APPLYCREDITTYPE, 
                        src:BILLDESC::varchar AS BILLDESC, 
                        src:BILLINGFAMILY::integer AS BILLINGFAMILY, 
                        src:BILLMAXIMUM::numeric(38, 10) AS BILLMAXIMUM, 
                        src:BILLMINIMUM::numeric(38, 10) AS BILLMINIMUM, 
                        src:BILLTYPE::varchar AS BILLTYPE, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CONDITIONFORMULAKEY::integer AS CONDITIONFORMULAKEY, 
                        src:CORRPROCESSSETUP::integer AS CORRPROCESSSETUP, 
                        src:DEFAULTFORREFUND::varchar AS DEFAULTFORREFUND, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENCYEXEMPTFLAG::varchar AS DELINQUENCYEXEMPTFLAG, 
                        src:DELINQUENCYTYPE::integer AS DELINQUENCYTYPE, 
                        src:DELINQUENTDATEDAYS::integer AS DELINQUENTDATEDAYS, 
                        src:DELINQUENTDATEFORMULAKEY::integer AS DELINQUENTDATEFORMULAKEY, 
                        src:DISCNTPENDAYFORMULAKEY::integer AS DISCNTPENDAYFORMULAKEY, 
                        src:DISCOUNTPENALTYDAYS::integer AS DISCOUNTPENALTYDAYS, 
                        src:DISCOUNTPENALTYLISUKEY::integer AS DISCOUNTPENALTYLISUKEY, 
                        src:DISCOUNTPENALTYTYPE::varchar AS DISCOUNTPENALTYTYPE, 
                        src:DISCOUNTPENALTYVARIANCE::numeric(38, 10) AS DISCOUNTPENALTYVARIANCE, 
                        src:DUEDATEDAYS::integer AS DUEDATEDAYS, 
                        src:DUEDATEFORMULAKEY::integer AS DUEDATEFORMULAKEY, 
                        src:DYNAMICBPFLAG::varchar AS DYNAMICBPFLAG, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INCLUDESERVICEDATES::integer AS INCLUDESERVICEDATES, 
                        src:INTERIMTYPEFLAG::varchar AS INTERIMTYPEFLAG, 
                        src:MINCHARGEAMOUNT::numeric(38, 10) AS MINCHARGEAMOUNT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYDEPOSITFIRSTFLAG::varchar AS PAYDEPOSITFIRSTFLAG, 
                        src:PAYMENTAPPLICATIONRULE::integer AS PAYMENTAPPLICATIONRULE, 
                        src:PAYMENTORDER::integer AS PAYMENTORDER, 
                        src:PENALIZEENTIREBILLFLAG::varchar AS PENALIZEENTIREBILLFLAG, 
                        src:PENALLFLAG::varchar AS PENALLFLAG, 
                        src:PENALTYAGEDFLAG::varchar AS PENALTYAGEDFLAG, 
                        src:PENALTYCODEKEY::integer AS PENALTYCODEKEY, 
                        src:PENALTYPENFLAG::varchar AS PENALTYPENFLAG, 
                        src:READINGSTRACKED::integer AS READINGSTRACKED, 
                        src:USEWINTERAVERAGEUSAGEONLYFLAG::varchar AS USEWINTERAVERAGEUSAGEONLYFLAG, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VIEWBILLIMAGEFORMULA::integer AS VIEWBILLIMAGEFORMULA, 
                        src:WAIVEPENALTYDEFAULTCODE::integer AS WAIVEPENALTYDEFAULTCODE, 
                        src:WAIVEPENALTYLOG::varchar AS WAIVEPENALTYLOG, 
                        src:WHERECLAUSE::varchar AS WHERECLAUSE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:BILLTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILLTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLYCREDITTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLINGFAMILY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLMAXIMUM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLMINIMUM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CORRPROCESSSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENCYTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENTDATEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENTDATEFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCNTPENDAYFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCOUNTPENALTYDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCOUNTPENALTYLISUKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCOUNTPENALTYVARIANCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DUEDATEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DUEDATEFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INCLUDESERVICEDATES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINCHARGEAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTAPPLICATIONRULE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PENALTYCODEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGSTRACKED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VIEWBILLIMAGEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WAIVEPENALTYDEFAULTCODE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null