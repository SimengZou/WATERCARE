CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_MOVEINSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRSECTORDER::integer AS ADDRSECTORDER, 
                        src:BILLINGSTATUS::varchar AS BILLINGSTATUS, 
                        src:CANCELCONFIRMATIONMSGKEY::integer AS CANCELCONFIRMATIONMSGKEY, 
                        src:CANCELLOGTYPE::varchar AS CANCELLOGTYPE, 
                        src:CANCELREQUESTRESOLUTION::varchar AS CANCELREQUESTRESOLUTION, 
                        src:CDRBUILDINGAPPLTYPE::integer AS CDRBUILDINGAPPLTYPE, 
                        src:CONFIRMCNTCTINFOSECTORDER::integer AS CONFIRMCNTCTINFOSECTORDER, 
                        src:CONTACTSECTORDER::integer AS CONTACTSECTORDER, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXISTCUSTDEFAULTFLAG::varchar AS EXISTCUSTDEFAULTFLAG, 
                        src:EXISTCUSTPROMPT::varchar AS EXISTCUSTPROMPT, 
                        src:FINALIZEACTIVEACCTFLAG::varchar AS FINALIZEACTIVEACCTFLAG, 
                        src:MAXDAYSPASTTOALLOWCANCEL::integer AS MAXDAYSPASTTOALLOWCANCEL, 
                        src:MAXDAYSPASTTOALLOWMODIFY::integer AS MAXDAYSPASTTOALLOWMODIFY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODIFYCONFIRMATIONMSGKEY::integer AS MODIFYCONFIRMATIONMSGKEY, 
                        src:MODIFYLOGTYPE::varchar AS MODIFYLOGTYPE, 
                        src:MODIFYMAXINTFORNEWREQUEST::integer AS MODIFYMAXINTFORNEWREQUEST, 
                        src:MOVEINDATEPROMPT::varchar AS MOVEINDATEPROMPT, 
                        src:MOVEINDATESECTORDER::integer AS MOVEINDATESECTORDER, 
                        src:MOVEINSETUPKEY::integer AS MOVEINSETUPKEY, 
                        src:OCCUPANTPROMPT::varchar AS OCCUPANTPROMPT, 
                        src:OPTSERVSECTORDER::integer AS OPTSERVSECTORDER, 
                        src:PROVSERVSECTORDER::integer AS PROVSERVSECTORDER, 
                        src:READINGREQUESTRESOLUTION::varchar AS READINGREQUESTRESOLUTION, 
                        src:SEARCHCALLERPROMPT::varchar AS SEARCHCALLERPROMPT, 
                        src:SEARCHEMAILFLAG::varchar AS SEARCHEMAILFLAG, 
                        src:SEARCHEMAILORDER::integer AS SEARCHEMAILORDER, 
                        src:SEARCHEMAILTITLE::varchar AS SEARCHEMAILTITLE, 
                        src:SEARCHFIRSTNAMEFLAG::varchar AS SEARCHFIRSTNAMEFLAG, 
                        src:SEARCHFIRSTNAMEORDER::integer AS SEARCHFIRSTNAMEORDER, 
                        src:SEARCHFIRSTNAMETITLE::varchar AS SEARCHFIRSTNAMETITLE, 
                        src:SEARCHIDFLAG::varchar AS SEARCHIDFLAG, 
                        src:SEARCHIDORDER::integer AS SEARCHIDORDER, 
                        src:SEARCHIDTITLE::varchar AS SEARCHIDTITLE, 
                        src:SEARCHLASTNAMEFLAG::varchar AS SEARCHLASTNAMEFLAG, 
                        src:SEARCHLASTNAMEORDER::integer AS SEARCHLASTNAMEORDER, 
                        src:SEARCHLASTNAMETITLE::varchar AS SEARCHLASTNAMETITLE, 
                        src:SEARCHLICENSEFLAG::varchar AS SEARCHLICENSEFLAG, 
                        src:SEARCHLICENSEORDER::integer AS SEARCHLICENSEORDER, 
                        src:SEARCHLICENSETITLE::varchar AS SEARCHLICENSETITLE, 
                        src:SEARCHPHONEFLAG::varchar AS SEARCHPHONEFLAG, 
                        src:SEARCHPHONEORDER::integer AS SEARCHPHONEORDER, 
                        src:SEARCHPHONETITLE::varchar AS SEARCHPHONETITLE, 
                        src:SECONDOWNERMSGKEY::integer AS SECONDOWNERMSGKEY, 
                        src:SECONDTENANTMSGKEY::integer AS SECONDTENANTMSGKEY, 
                        src:SERVICESSECTORDER::integer AS SERVICESSECTORDER, 
                        src:SERVICESTATUS::varchar AS SERVICESTATUS, 
                        src:SHOWADDRALERTSFLAG::varchar AS SHOWADDRALERTSFLAG, 
                        src:SHOWADDRSECTFLAG::varchar AS SHOWADDRSECTFLAG, 
                        src:SHOWCONFIRMCNTCTINFOFLAG::varchar AS SHOWCONFIRMCNTCTINFOFLAG, 
                        src:SHOWCUSTALERTSFLAG::varchar AS SHOWCUSTALERTSFLAG, 
                        src:SHOWEXISTCNTCTCOMMFLAG::varchar AS SHOWEXISTCNTCTCOMMFLAG, 
                        src:SHOWEXISTCNTCTSEASADDRFLAG::varchar AS SHOWEXISTCNTCTSEASADDRFLAG, 
                        src:SHOWEXISTCUSTCOMMFLAG::varchar AS SHOWEXISTCUSTCOMMFLAG, 
                        src:SHOWEXISTCUSTFIELDSFLAG::varchar AS SHOWEXISTCUSTFIELDSFLAG, 
                        src:SHOWEXISTCUSTPROMPTFLAG::varchar AS SHOWEXISTCUSTPROMPTFLAG, 
                        src:SHOWLOCBYSERVADDRLINKFLAG::varchar AS SHOWLOCBYSERVADDRLINKFLAG, 
                        src:SHOWNEWCNTCTCOMMFLAG::varchar AS SHOWNEWCNTCTCOMMFLAG, 
                        src:SHOWNEWCNTCTSEASADDRFLAG::varchar AS SHOWNEWCNTCTSEASADDRFLAG, 
                        src:SHOWNEWCUSTCOMMFLAG::varchar AS SHOWNEWCUSTCOMMFLAG, 
                        src:TENANTNOOWNERMSGKEY::integer AS TENANTNOOWNERMSGKEY, 
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
    
                        
                src:MOVEINSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MOVEINSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_MOVEINSETUP as strm))