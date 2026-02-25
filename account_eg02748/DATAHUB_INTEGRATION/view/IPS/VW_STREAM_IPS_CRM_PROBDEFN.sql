CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CRM_PROBDEFN AS SELECT
                        src:ACCTFLAG::varchar AS ACCTFLAG, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRREQD::varchar AS ADDRREQD, 
                        src:ALLOWASSETINSP::varchar AS ALLOWASSETINSP, 
                        src:ALLOWCASE::varchar AS ALLOWCASE, 
                        src:ALLOWOOC::varchar AS ALLOWOOC, 
                        src:ALLOWWORKORDER::varchar AS ALLOWWORKORDER, 
                        src:ASSETINSPTYPE::integer AS ASSETINSPTYPE, 
                        src:ASSETREQD::varchar AS ASSETREQD, 
                        src:ASSETTYPE::integer AS ASSETTYPE, 
                        src:ASSIGNNOTICE::varchar AS ASSIGNNOTICE, 
                        src:AUTORES::integer AS AUTORES, 
                        src:CALLERFLG::varchar AS CALLERFLG, 
                        src:CASERES::varchar AS CASERES, 
                        src:CASETYPE::integer AS CASETYPE, 
                        src:CATEGORY::integer AS CATEGORY, 
                        src:CHECKDUPLICATELOCALONLY::varchar AS CHECKDUPLICATELOCALONLY, 
                        src:CHKDUPFLG::varchar AS CHKDUPFLG, 
                        src:CORRPROCESSSETUP::integer AS CORRPROCESSSETUP, 
                        src:CREATEDINLASTXMIN::integer AS CREATEDINLASTXMIN, 
                        src:CUSTFLAG::varchar AS CUSTFLAG, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DETAILPAGE::varchar AS DETAILPAGE, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FLLWBYBUS::varchar AS FLLWBYBUS, 
                        src:GISICON::varchar AS GISICON, 
                        src:HOLIDAY::varchar AS HOLIDAY, 
                        src:HOWTOCREATECASE::varchar AS HOWTOCREATECASE, 
                        src:HOWTOCREATEINSP::varchar AS HOWTOCREATEINSP, 
                        src:HOWTOCREATEOOC::varchar AS HOWTOCREATEOOC, 
                        src:HOWTOCREATEWO::varchar AS HOWTOCREATEWO, 
                        src:INCLACCTNUMINOOC::varchar AS INCLACCTNUMINOOC, 
                        src:INCLADDRINCASE::varchar AS INCLADDRINCASE, 
                        src:INCLADDRINWO::varchar AS INCLADDRINWO, 
                        src:INCLASSETININSP::varchar AS INCLASSETININSP, 
                        src:INCLASSETINWO::varchar AS INCLASSETINWO, 
                        src:INCLCOMMENTSINCASE::varchar AS INCLCOMMENTSINCASE, 
                        src:INCLCOMMENTSININSP::varchar AS INCLCOMMENTSININSP, 
                        src:INCLCOMMENTSINOOC::varchar AS INCLCOMMENTSINOOC, 
                        src:INCLCOMMENTSINWO::varchar AS INCLCOMMENTSINWO, 
                        src:INCLCONTACTINCASE::varchar AS INCLCONTACTINCASE, 
                        src:INCLCONTACTINWO::varchar AS INCLCONTACTINWO, 
                        src:INCLLOCINCASE::varchar AS INCLLOCINCASE, 
                        src:INCLLOCINWO::varchar AS INCLLOCINWO, 
                        src:INSPCTRTY::varchar AS INSPCTRTY, 
                        src:INSPDAYS::integer AS INSPDAYS, 
                        src:INSPECTNOTICE::varchar AS INSPECTNOTICE, 
                        src:INSPECTORNOTICE::varchar AS INSPECTORNOTICE, 
                        src:INSPHRS::integer AS INSPHRS, 
                        src:INSPMINS::integer AS INSPMINS, 
                        src:INSPRES::varchar AS INSPRES, 
                        src:ISPUBLIC::varchar AS ISPUBLIC, 
                        src:LOGGEDPERSONNOTICE::varchar AS LOGGEDPERSONNOTICE, 
                        src:METERRESCODE::varchar AS METERRESCODE, 
                        src:METERSERVSTAT::varchar AS METERSERVSTAT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTIFYCUSTOMER::varchar AS NOTIFYCUSTOMER, 
                        src:OOCRES::varchar AS OOCRES, 
                        src:OOCTYPE::integer AS OOCTYPE, 
                        src:OVERRIDEGLOBALSETTINGFLG::varchar AS OVERRIDEGLOBALSETTINGFLG, 
                        src:POOLKEY::integer AS POOLKEY, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:PORTALSEARCHKEYWORDS::varchar AS PORTALSEARCHKEYWORDS, 
                        src:PRI::varchar AS PRI, 
                        src:PRNUSGHIST::varchar AS PRNUSGHIST, 
                        src:PROBCODE::varchar AS PROBCODE, 
                        src:PROBDESC::varchar AS PROBDESC, 
                        src:PROBGRP::varchar AS PROBGRP, 
                        src:PROBKEY::integer AS PROBKEY, 
                        src:PROJECT::varchar AS PROJECT, 
                        src:RESDAYS::integer AS RESDAYS, 
                        src:RESHRS::integer AS RESHRS, 
                        src:RESOLVENOTICE::varchar AS RESOLVENOTICE, 
                        src:RESP::varchar AS RESP, 
                        src:SAMEREQUESTTYPEFLG::varchar AS SAMEREQUESTTYPEFLG, 
                        src:SCHEDDAYS::integer AS SCHEDDAYS, 
                        src:SCHEDHRS::integer AS SCHEDHRS, 
                        src:SCHEDNOTICE::varchar AS SCHEDNOTICE, 
                        src:SEARCHRADIUS::numeric(38, 10) AS SEARCHRADIUS, 
                        src:SERVREQASSETTYPE::integer AS SERVREQASSETTYPE, 
                        src:SHOWINBILLPORTAL::varchar AS SHOWINBILLPORTAL, 
                        src:SHOWINPORTAL::varchar AS SHOWINPORTAL, 
                        src:SHOWUSAGEDETAILS::varchar AS SHOWUSAGEDETAILS, 
                        src:STANDARDWORKORDERKEY::integer AS STANDARDWORKORDERKEY, 
                        src:STARTDAYS::integer AS STARTDAYS, 
                        src:STARTHRS::integer AS STARTHRS, 
                        src:STDRES::varchar AS STDRES, 
                        src:SUBMITINBILLPORTAL::varchar AS SUBMITINBILLPORTAL, 
                        src:SUBMITINPORTAL::varchar AS SUBMITINPORTAL, 
                        src:SUBMITINPORTALPUBLIC::varchar AS SUBMITINPORTALPUBLIC, 
                        src:SUSPDAYS::integer AS SUSPDAYS, 
                        src:SUSPHRS::integer AS SUSPHRS, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WEEKDAY::varchar AS WEEKDAY, 
                        src:WEEKEND::varchar AS WEEKEND, 
                        src:WHENTOCREATECASE::varchar AS WHENTOCREATECASE, 
                        src:WHENTOCREATEINSP::varchar AS WHENTOCREATEINSP, 
                        src:WHENTOCREATEOOC::varchar AS WHENTOCREATEOOC, 
                        src:WHENTOCREATEWO::varchar AS WHENTOCREATEWO, 
                        src:WOACTIVITY::integer AS WOACTIVITY, 
                        src:WORES::varchar AS WORES, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:PROBKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CRM_PROBDEFN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSETINSPTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTORES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CASETYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CATEGORY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CORRPROCESSSETUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CREATEDINLASTXMIN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPMINS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OOCTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:POOLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROBKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEARCHRADIUS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVREQASSETTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STANDARDWORKORDERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STARTDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STARTHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SUSPDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SUSPHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WOACTIVITY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null