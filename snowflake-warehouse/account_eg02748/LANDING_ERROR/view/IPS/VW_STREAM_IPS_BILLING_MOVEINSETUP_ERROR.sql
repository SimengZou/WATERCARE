CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_MOVEINSETUP_ERROR AS SELECT src, 'IPS_BILLING_MOVEINSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRSECTORDER), '0'), 38, 10) is null and 
                    src:ADDRSECTORDER is not null) THEN
                    'ADDRSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:CANCELCONFIRMATIONMSGKEY is not null) THEN
                    'CANCELCONFIRMATIONMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRBUILDINGAPPLTYPE), '0'), 38, 10) is null and 
                    src:CDRBUILDINGAPPLTYPE is not null) THEN
                    'CDRBUILDINGAPPLTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:CDRBUILDINGAPPLTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONFIRMCNTCTINFOSECTORDER), '0'), 38, 10) is null and 
                    src:CONFIRMCNTCTINFOSECTORDER is not null) THEN
                    'CONFIRMCNTCTINFOSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CONFIRMCNTCTINFOSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTSECTORDER), '0'), 38, 10) is null and 
                    src:CONTACTSECTORDER is not null) THEN
                    'CONTACTSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACTSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWCANCEL is not null) THEN
                    'MAXDAYSPASTTOALLOWCANCEL contains a non-numeric value : \'' || AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWMODIFY is not null) THEN
                    'MAXDAYSPASTTOALLOWMODIFY contains a non-numeric value : \'' || AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:MODIFYCONFIRMATIONMSGKEY is not null) THEN
                    'MODIFYCONFIRMATIONMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST), '0'), 38, 10) is null and 
                    src:MODIFYMAXINTFORNEWREQUEST is not null) THEN
                    'MODIFYMAXINTFORNEWREQUEST contains a non-numeric value : \'' || AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINDATESECTORDER), '0'), 38, 10) is null and 
                    src:MOVEINDATESECTORDER is not null) THEN
                    'MOVEINDATESECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINDATESECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSETUPKEY), '0'), 38, 10) is null and 
                    src:MOVEINSETUPKEY is not null) THEN
                    'MOVEINSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPTSERVSECTORDER), '0'), 38, 10) is null and 
                    src:OPTSERVSECTORDER is not null) THEN
                    'OPTSERVSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:OPTSERVSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROVSERVSECTORDER), '0'), 38, 10) is null and 
                    src:PROVSERVSECTORDER is not null) THEN
                    'PROVSERVSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PROVSERVSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHEMAILORDER), '0'), 38, 10) is null and 
                    src:SEARCHEMAILORDER is not null) THEN
                    'SEARCHEMAILORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHEMAILORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHFIRSTNAMEORDER), '0'), 38, 10) is null and 
                    src:SEARCHFIRSTNAMEORDER is not null) THEN
                    'SEARCHFIRSTNAMEORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHFIRSTNAMEORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHIDORDER), '0'), 38, 10) is null and 
                    src:SEARCHIDORDER is not null) THEN
                    'SEARCHIDORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHIDORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHLASTNAMEORDER), '0'), 38, 10) is null and 
                    src:SEARCHLASTNAMEORDER is not null) THEN
                    'SEARCHLASTNAMEORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHLASTNAMEORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHLICENSEORDER), '0'), 38, 10) is null and 
                    src:SEARCHLICENSEORDER is not null) THEN
                    'SEARCHLICENSEORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHLICENSEORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHPHONEORDER), '0'), 38, 10) is null and 
                    src:SEARCHPHONEORDER is not null) THEN
                    'SEARCHPHONEORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHPHONEORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SECONDOWNERMSGKEY), '0'), 38, 10) is null and 
                    src:SECONDOWNERMSGKEY is not null) THEN
                    'SECONDOWNERMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SECONDOWNERMSGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SECONDTENANTMSGKEY), '0'), 38, 10) is null and 
                    src:SECONDTENANTMSGKEY is not null) THEN
                    'SECONDTENANTMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SECONDTENANTMSGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICESSECTORDER), '0'), 38, 10) is null and 
                    src:SERVICESSECTORDER is not null) THEN
                    'SERVICESSECTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICESSECTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TENANTNOOWNERMSGKEY), '0'), 38, 10) is null and 
                    src:TENANTNOOWNERMSGKEY is not null) THEN
                    'TENANTNOOWNERMSGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:TENANTNOOWNERMSGKEY) || '\'' WHEN 
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
                                    
                src:MOVEINSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_MOVEINSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRSECTORDER), '0'), 38, 10) is null and 
                    src:ADDRSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:CANCELCONFIRMATIONMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRBUILDINGAPPLTYPE), '0'), 38, 10) is null and 
                    src:CDRBUILDINGAPPLTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONFIRMCNTCTINFOSECTORDER), '0'), 38, 10) is null and 
                    src:CONFIRMCNTCTINFOSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTSECTORDER), '0'), 38, 10) is null and 
                    src:CONTACTSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWCANCEL), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWCANCEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXDAYSPASTTOALLOWMODIFY), '0'), 38, 10) is null and 
                    src:MAXDAYSPASTTOALLOWMODIFY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYCONFIRMATIONMSGKEY), '0'), 38, 10) is null and 
                    src:MODIFYCONFIRMATIONMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MODIFYMAXINTFORNEWREQUEST), '0'), 38, 10) is null and 
                    src:MODIFYMAXINTFORNEWREQUEST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINDATESECTORDER), '0'), 38, 10) is null and 
                    src:MOVEINDATESECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSETUPKEY), '0'), 38, 10) is null and 
                    src:MOVEINSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPTSERVSECTORDER), '0'), 38, 10) is null and 
                    src:OPTSERVSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROVSERVSECTORDER), '0'), 38, 10) is null and 
                    src:PROVSERVSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHEMAILORDER), '0'), 38, 10) is null and 
                    src:SEARCHEMAILORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHFIRSTNAMEORDER), '0'), 38, 10) is null and 
                    src:SEARCHFIRSTNAMEORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHIDORDER), '0'), 38, 10) is null and 
                    src:SEARCHIDORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHLASTNAMEORDER), '0'), 38, 10) is null and 
                    src:SEARCHLASTNAMEORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHLICENSEORDER), '0'), 38, 10) is null and 
                    src:SEARCHLICENSEORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHPHONEORDER), '0'), 38, 10) is null and 
                    src:SEARCHPHONEORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SECONDOWNERMSGKEY), '0'), 38, 10) is null and 
                    src:SECONDOWNERMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SECONDTENANTMSGKEY), '0'), 38, 10) is null and 
                    src:SECONDTENANTMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICESSECTORDER), '0'), 38, 10) is null and 
                    src:SERVICESSECTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TENANTNOOWNERMSGKEY), '0'), 38, 10) is null and 
                    src:TENANTNOOWNERMSGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)