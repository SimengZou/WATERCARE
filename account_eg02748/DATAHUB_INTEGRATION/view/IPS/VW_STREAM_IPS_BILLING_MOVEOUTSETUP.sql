CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_MOVEOUTSETUP AS SELECT
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
                                        
                src:MOVEOUTSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_MOVEOUTSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCTPREFSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CANCELCNFRMCNTCTSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CANCELSERVICESSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONFIRMCNTCTSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENCYSCHEMEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FORWARDINFOSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FOWARDCNTCTSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXTENANTINTERVALDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MODIFYSERVICESSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MODMOVEOUTCNTCTSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTINSTRSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTSETUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEWACCTSRVSSECTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null