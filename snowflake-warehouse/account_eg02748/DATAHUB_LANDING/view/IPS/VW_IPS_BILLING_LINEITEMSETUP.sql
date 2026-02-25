CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_LINEITEMSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLYDEPOSITFLAG::varchar AS APPLYDEPOSITFLAG, 
                        src:APPROVALGROUP::integer AS APPROVALGROUP, 
                        src:APPROVALSCHEME::integer AS APPROVALSCHEME, 
                        src:AUTOMATEDFLAG::varchar AS AUTOMATEDFLAG, 
                        src:BBSETLFLAG::varchar AS BBSETLFLAG, 
                        src:BILLINGFAMILY::integer AS BILLINGFAMILY, 
                        src:BUDGETBILLINGVARIANTFLAG::varchar AS BUDGETBILLINGVARIANTFLAG, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CONDITIONFORMULAKEY::integer AS CONDITIONFORMULAKEY, 
                        src:DEFAULTADJUSTMENTSETUPKEY::integer AS DEFAULTADJUSTMENTSETUPKEY, 
                        src:DEFAULTQUANTITY::integer AS DEFAULTQUANTITY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENCYEXEMPTFLAG::varchar AS DELINQUENCYEXEMPTFLAG, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:DESIGNATION::varchar AS DESIGNATION, 
                        src:DONATIONFLAG::varchar AS DONATIONFLAG, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FEETYPEKEY::integer AS FEETYPEKEY, 
                        src:FORTRANSFERFLAG::varchar AS FORTRANSFERFLAG, 
                        src:FORTRANSFERPAYMENTFLAG::varchar AS FORTRANSFERPAYMENTFLAG, 
                        src:INCLUDEINBUDGETEDAMOUNTFLAG::varchar AS INCLUDEINBUDGETEDAMOUNTFLAG, 
                        src:INITIALDEPOSITCHARGEFLAG::varchar AS INITIALDEPOSITCHARGEFLAG, 
                        src:ISPRINTTEXTEDITABLE::varchar AS ISPRINTTEXTEDITABLE, 
                        src:LINEITEM::varchar AS LINEITEM, 
                        src:LINEITEMGROUP::varchar AS LINEITEMGROUP, 
                        src:LINEITEMSETUPKEY::integer AS LINEITEMSETUPKEY, 
                        src:LINEITEMTYPE::varchar AS LINEITEMTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTIDISTRIBUTIOLNALFLAG::varchar AS MULTIDISTRIBUTIOLNALFLAG, 
                        src:NUMBEROFPROMPTPAIDBILLS::integer AS NUMBEROFPROMPTPAIDBILLS, 
                        src:ONEOFFOPTIONINDICATOR::integer AS ONEOFFOPTIONINDICATOR, 
                        src:PENALTYCODEKEY::integer AS PENALTYCODEKEY, 
                        src:PRINTTEXT::varchar AS PRINTTEXT, 
                        src:PROMPTPAIDPERIOD::integer AS PROMPTPAIDPERIOD, 
                        src:PROPERTYBASEDFLAG::varchar AS PROPERTYBASEDFLAG, 
                        src:QUANTITYBASEDFLAG::varchar AS QUANTITYBASEDFLAG, 
                        src:RATECHANGEALGORITHM::integer AS RATECHANGEALGORITHM, 
                        src:RATECODEKEY::integer AS RATECODEKEY, 
                        src:REBATEGROUP::varchar AS REBATEGROUP, 
                        src:ROUNDTO::numeric(38, 10) AS ROUNDTO, 
                        src:SERVICEOPTIONSKEY::integer AS SERVICEOPTIONSKEY, 
                        src:SETTLEMENTLINEITEMSETUPKEY::integer AS SETTLEMENTLINEITEMSETUPKEY, 
                        src:SRVBASEDOPTIONINDICATOR::integer AS SRVBASEDOPTIONINDICATOR, 
                        src:TAXAREA::varchar AS TAXAREA, 
                        src:TAXINCLUSIVEFLAG::varchar AS TAXINCLUSIVEFLAG, 
                        src:TYPEINDICATOR::integer AS TYPEINDICATOR, 
                        src:VALUEBASEDFLAG::varchar AS VALUEBASEDFLAG, 
                        src:VALUEFORMULAKEY::integer AS VALUEFORMULAKEY, 
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
    
                        
                src:LINEITEMSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LINEITEMSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_LINEITEMSETUP as strm))