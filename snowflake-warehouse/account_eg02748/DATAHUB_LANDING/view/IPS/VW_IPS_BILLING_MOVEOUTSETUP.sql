CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_MOVEOUTSETUP AS SELECT
                        src:ACCTPREFSECTORDER::integer AS ACCTPREFSECTORDER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLINGSTATUS::varchar AS BILLINGSTATUS, 
                        src:CANCELCNFRMCNTCTSECTORDER::integer AS CANCELCNFRMCNTCTSECTORDER, 
                        src:CANCELCONFIRMATIONMSGKEY::integer AS CANCELCONFIRMATIONMSGKEY, 
                        src:CANCELLOGTYPE::varchar AS CANCELLOGTYPE, 
                        src:CANCELREQUESTRESOLUTION::varchar AS CANCELREQUESTRESOLUTION, 
                        src:CANCELSERVICESSECTORDER::integer AS CANCELSERVICESSECTORDER, 
                        src:CONFIRMCNTCTSECTORDER::integer AS CONFIRMCNTCTSECTORDER, 
                        src:COPYCNTCTSDEFAULTFLAG::varchar AS COPYCNTCTSDEFAULTFLAG, 
                        src:COPYCNTCTSPROMPT::varchar AS COPYCNTCTSPROMPT, 
                        src:COPYWAFROMPREVACCTFLAG::varchar AS COPYWAFROMPREVACCTFLAG, 
                        src:CREATEACCTDEFAULTFLAG::varchar AS CREATEACCTDEFAULTFLAG, 
                        src:CREATEACCTPROMPT::varchar AS CREATEACCTPROMPT, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENCYSCHEMEKEY::integer AS DELINQUENCYSCHEMEKEY, 
                        src:DIRECTDEBITPROMPT::varchar AS DIRECTDEBITPROMPT, 
                        src:FORWARDINFOPROMPT::varchar AS FORWARDINFOPROMPT, 
                        src:FORWARDINFOSECTORDER::integer AS FORWARDINFOSECTORDER, 
                        src:FOWARDCNTCTSECTORDER::integer AS FOWARDCNTCTSECTORDER, 
                        src:LOGTYPECODE::varchar AS LOGTYPECODE, 
                        src:MAXDAYSPASTTOALLOWCANCEL::integer AS MAXDAYSPASTTOALLOWCANCEL, 
                        src:MAXDAYSPASTTOALLOWMODIFY::integer AS MAXDAYSPASTTOALLOWMODIFY, 
                        src:MAXTENANTINTERVALDAYS::integer AS MAXTENANTINTERVALDAYS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODIFYCONFIRMATIONMSGKEY::integer AS MODIFYCONFIRMATIONMSGKEY, 
                        src:MODIFYLOGTYPE::varchar AS MODIFYLOGTYPE, 
                        src:MODIFYMAXINTFORNEWREQUEST::integer AS MODIFYMAXINTFORNEWREQUEST, 
                        src:MODIFYSERVICESSECTORDER::integer AS MODIFYSERVICESSECTORDER, 
                        src:MODMOVEOUTCNTCTSECTORDER::integer AS MODMOVEOUTCNTCTSECTORDER, 
                        src:MOVEINDATEPROMPT::varchar AS MOVEINDATEPROMPT, 
                        src:MOVEOUTDATEPROMPT::varchar AS MOVEOUTDATEPROMPT, 
                        src:MOVEOUTINSTRSECTORDER::integer AS MOVEOUTINSTRSECTORDER, 
                        src:MOVEOUTSETUPKEY::integer AS MOVEOUTSETUPKEY, 
                        src:MOVEOUTSRVINSTFLAG::varchar AS MOVEOUTSRVINSTFLAG, 
                        src:MOVETOADDRPROMPT::varchar AS MOVETOADDRPROMPT, 
                        src:NEWACCTSRVSSECTORDER::integer AS NEWACCTSRVSSECTORDER, 
                        src:NEWSRVFLAG::varchar AS NEWSRVFLAG, 
                        src:READINGREQUESTRESOLUTION::varchar AS READINGREQUESTRESOLUTION, 
                        src:SERVICESTATUS::varchar AS SERVICESTATUS, 
                        src:SHOWCANCELCNFRMCNTCTFLAG::varchar AS SHOWCANCELCNFRMCNTCTFLAG, 
                        src:SHOWCONFIRMCNTCTINFOFLAG::varchar AS SHOWCONFIRMCNTCTINFOFLAG, 
                        src:SHOWMOVEINADDRALRTSFLAG::varchar AS SHOWMOVEINADDRALRTSFLAG, 
                        src:STAYINAREADEFAULTFLAG::varchar AS STAYINAREADEFAULTFLAG, 
                        src:STAYINAREAPROMPT::varchar AS STAYINAREAPROMPT, 
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
    
                        
                src:MOVEOUTSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MOVEOUTSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_MOVEOUTSETUP as strm))