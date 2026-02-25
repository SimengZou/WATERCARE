CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_MOVEOUTSETUP_ERROR AS SELECT src, 'IPS_BILLING_MOVEOUTSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTPREFSECTORDER), '0'), 38, 10) is null and 
                    src:ACCTPREFSECTORDER is not null) THEN
                    'ACCTPREFSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTPREFSECTORDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELCNFRMCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:CANCELCNFRMCNTCTSECTORDER is not null) THEN
                    'CANCELCNFRMCNTCTSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CANCELCNFRMCNTCTSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:CANCELCONFIRMATIONMSGKEY is not null) THEN
                    'CANCELCONFIRMATIONMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELSERVICESSECTORDER), '0'), 38, 10) is null and 
                    src:CANCELSERVICESSECTORDER is not null) THEN
                    'CANCELSERVICESSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CANCELSERVICESSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONFIRMCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:CONFIRMCNTCTSECTORDER is not null) THEN
                    'CONFIRMCNTCTSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CONFIRMCNTCTSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYSCHEMEKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYSCHEMEKEY is not null) THEN
                    'DELINQUENCYSCHEMEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENCYSCHEMEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORWARDINFOSECTORDER), '0'), 38, 10) is null and 
                    src:FORWARDINFOSECTORDER is not null) THEN
                    'FORWARDINFOSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:FORWARDINFOSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FOWARDCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:FOWARDCNTCTSECTORDER is not null) THEN
                    'FOWARDCNTCTSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:FOWARDCNTCTSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWCANCEL is not null) THEN
                    'MAXDAYSPASTTOALLOWCANCEL contains a non-numeric value : \'' || AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWMODIFY is not null) THEN
                    'MAXDAYSPASTTOALLOWMODIFY contains a non-numeric value : \'' || AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXTENANTINTERVALDAYS), '0'), 38, 10) is null and 
                    src:MAXTENANTINTERVALDAYS is not null) THEN
                    'MAXTENANTINTERVALDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:MAXTENANTINTERVALDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:MODIFYCONFIRMATIONMSGKEY is not null) THEN
                    'MODIFYCONFIRMATIONMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST), '0'), 38, 10) is null and 
                    src:MODIFYMAXINTFORNEWREQUEST is not null) THEN
                    'MODIFYMAXINTFORNEWREQUEST contains a non-numeric value : \'' || AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYSERVICESSECTORDER), '0'), 38, 10) is null and 
                    src:MODIFYSERVICESSECTORDER is not null) THEN
                    'MODIFYSERVICESSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:MODIFYSERVICESSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODMOVEOUTCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:MODMOVEOUTCNTCTSECTORDER is not null) THEN
                    'MODMOVEOUTCNTCTSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:MODMOVEOUTCNTCTSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTINSTRSECTORDER), '0'), 38, 10) is null and 
                    src:MOVEOUTINSTRSECTORDER is not null) THEN
                    'MOVEOUTINSTRSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTINSTRSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSETUPKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTSETUPKEY is not null) THEN
                    'MOVEOUTSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWACCTSRVSSECTORDER), '0'), 38, 10) is null and 
                    src:NEWACCTSRVSSECTORDER is not null) THEN
                    'NEWACCTSRVSSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:NEWACCTSRVSSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTPREFSECTORDER), '0'), 38, 10) is null and 
                    src:ACCTPREFSECTORDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELCNFRMCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:CANCELCNFRMCNTCTSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:CANCELCONFIRMATIONMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELSERVICESSECTORDER), '0'), 38, 10) is null and 
                    src:CANCELSERVICESSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONFIRMCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:CONFIRMCNTCTSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYSCHEMEKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYSCHEMEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORWARDINFOSECTORDER), '0'), 38, 10) is null and 
                    src:FORWARDINFOSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FOWARDCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:FOWARDCNTCTSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWCANCEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWMODIFY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXTENANTINTERVALDAYS), '0'), 38, 10) is null and 
                    src:MAXTENANTINTERVALDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:MODIFYCONFIRMATIONMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST), '0'), 38, 10) is null and 
                    src:MODIFYMAXINTFORNEWREQUEST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYSERVICESSECTORDER), '0'), 38, 10) is null and 
                    src:MODIFYSERVICESSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODMOVEOUTCNTCTSECTORDER), '0'), 38, 10) is null and 
                    src:MODMOVEOUTCNTCTSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTINSTRSECTORDER), '0'), 38, 10) is null and 
                    src:MOVEOUTINSTRSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSETUPKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWACCTSRVSSECTORDER), '0'), 38, 10) is null and 
                    src:NEWACCTSRVSSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)