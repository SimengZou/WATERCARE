CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WOLABORSCHEDPARAMS AS SELECT
                        src:WLS_ACTENDDATE::datetime AS WLS_ACTENDDATE, 
                        src:WLS_ACTSTARTDATE::datetime AS WLS_ACTSTARTDATE, 
                        src:WLS_ACTTASK::varchar AS WLS_ACTTASK, 
                        src:WLS_ACTTRADE::varchar AS WLS_ACTTRADE, 
                        src:WLS_ALLOWACTSONDIFFSESSION::varchar AS WLS_ALLOWACTSONDIFFSESSION, 
                        src:WLS_AUTOSELECTACTIVITY::varchar AS WLS_AUTOSELECTACTIVITY, 
                        src:WLS_CAMPAIGN::varchar AS WLS_CAMPAIGN, 
                        src:WLS_CAMPAIGN_LINE::numeric(38, 10) AS WLS_CAMPAIGN_LINE, 
                        src:WLS_CODE::varchar AS WLS_CODE, 
                        src:WLS_CONSIST::varchar AS WLS_CONSIST, 
                        src:WLS_CREATED::datetime AS WLS_CREATED, 
                        src:WLS_DATALASTGEN::datetime AS WLS_DATALASTGEN, 
                        src:WLS_DEFPARAM::varchar AS WLS_DEFPARAM, 
                        src:WLS_EMPCLASS::varchar AS WLS_EMPCLASS, 
                        src:WLS_EMPCLASSORG::varchar AS WLS_EMPCLASSORG, 
                        src:WLS_EMPCREW::varchar AS WLS_EMPCREW, 
                        src:WLS_EMPCREWORG::varchar AS WLS_EMPCREWORG, 
                        src:WLS_EMPMRC::varchar AS WLS_EMPMRC, 
                        src:WLS_EMPSHIFT::varchar AS WLS_EMPSHIFT, 
                        src:WLS_EMPTRADE::varchar AS WLS_EMPTRADE, 
                        src:WLS_EVENT_ORG::varchar AS WLS_EVENT_ORG, 
                        src:WLS_EXCLUDEDURATION::varchar AS WLS_EXCLUDEDURATION, 
                        src:WLS_EXCLUDEPEOPLEREQUIRED::varchar AS WLS_EXCLUDEPEOPLEREQUIRED, 
                        src:WLS_FULLYSCHEDCOLOR::varchar AS WLS_FULLYSCHEDCOLOR, 
                        src:WLS_GENAVAILTHROUGH::datetime AS WLS_GENAVAILTHROUGH, 
                        src:WLS_LASTSAVED::datetime AS WLS_LASTSAVED, 
                        src:WLS_LIGHTLYSCHEDCOLOR::varchar AS WLS_LIGHTLYSCHEDCOLOR, 
                        src:WLS_LISTLASTREFRESHED::datetime AS WLS_LISTLASTREFRESHED, 
                        src:WLS_MAXCALCPRIORITY::numeric(38, 10) AS WLS_MAXCALCPRIORITY, 
                        src:WLS_MODERATELYSCHEDCOLOR::varchar AS WLS_MODERATELYSCHEDCOLOR, 
                        src:WLS_MP::varchar AS WLS_MP, 
                        src:WLS_MP_ORG::varchar AS WLS_MP_ORG, 
                        src:WLS_MP_SEQ::numeric(38, 10) AS WLS_MP_SEQ, 
                        src:WLS_OBJCATEGORY::varchar AS WLS_OBJCATEGORY, 
                        src:WLS_OBJCLASS::varchar AS WLS_OBJCLASS, 
                        src:WLS_OBJCLASSORG::varchar AS WLS_OBJCLASSORG, 
                        src:WLS_OBJCOSTCODE::varchar AS WLS_OBJCOSTCODE, 
                        src:WLS_OBJECT::varchar AS WLS_OBJECT, 
                        src:WLS_OBJINCLUDECHILDREN::varchar AS WLS_OBJINCLUDECHILDREN, 
                        src:WLS_OBJLOC::varchar AS WLS_OBJLOC, 
                        src:WLS_OBJLOCORG::varchar AS WLS_OBJLOCORG, 
                        src:WLS_OBJMRC::varchar AS WLS_OBJMRC, 
                        src:WLS_OBJORG::varchar AS WLS_OBJORG, 
                        src:WLS_OBJPARENT::varchar AS WLS_OBJPARENT, 
                        src:WLS_OBJPARENTORG::varchar AS WLS_OBJPARENTORG, 
                        src:WLS_OBJSTATUS::varchar AS WLS_OBJSTATUS, 
                        src:WLS_OBJTYPE::varchar AS WLS_OBJTYPE, 
                        src:WLS_OPERATIONALSTATUS::varchar AS WLS_OPERATIONALSTATUS, 
                        src:WLS_OVERSCHEDCOLOR::varchar AS WLS_OVERSCHEDCOLOR, 
                        src:WLS_PARAMETER::varchar AS WLS_PARAMETER, 
                        src:WLS_SCHEDULING::varchar AS WLS_SCHEDULING, 
                        src:WLS_SCHEDULINGSTARTDATE::datetime AS WLS_SCHEDULINGSTARTDATE, 
                        src:WLS_SESSIONID::numeric(38, 10) AS WLS_SESSIONID, 
                        src:WLS_SHIFTDURATION::numeric(38, 10) AS WLS_SHIFTDURATION, 
                        src:WLS_SHIFTSCHEDULING::varchar AS WLS_SHIFTSCHEDULING, 
                        src:WLS_SHIFTSTART::datetime AS WLS_SHIFTSTART, 
                        src:WLS_SKIPEQUIPSELECTIONPROCESS::varchar AS WLS_SKIPEQUIPSELECTIONPROCESS, 
                        src:WLS_THRESHOLDPERCENT::numeric(38, 10) AS WLS_THRESHOLDPERCENT, 
                        src:WLS_UNSCHEDCOLOR::varchar AS WLS_UNSCHEDCOLOR, 
                        src:WLS_UPDATECOUNT::numeric(38, 10) AS WLS_UPDATECOUNT, 
                        src:WLS_UPDATED::datetime AS WLS_UPDATED, 
                        src:WLS_USER::varchar AS WLS_USER, 
                        src:WLS_WOASSIGNEDBY::varchar AS WLS_WOASSIGNEDBY, 
                        src:WLS_WOASSIGNEDTO::varchar AS WLS_WOASSIGNEDTO, 
                        src:WLS_WOCLASS::varchar AS WLS_WOCLASS, 
                        src:WLS_WOCLASSORG::varchar AS WLS_WOCLASSORG, 
                        src:WLS_WOCRITICALITY::varchar AS WLS_WOCRITICALITY, 
                        src:WLS_WOJOBTYPE::varchar AS WLS_WOJOBTYPE, 
                        src:WLS_WOLOC::varchar AS WLS_WOLOC, 
                        src:WLS_WOLOCORG::varchar AS WLS_WOLOCORG, 
                        src:WLS_WOMRC::varchar AS WLS_WOMRC, 
                        src:WLS_WOOBJECT::varchar AS WLS_WOOBJECT, 
                        src:WLS_WOOBJECTORG::varchar AS WLS_WOOBJECTORG, 
                        src:WLS_WOPMSCHEDULE::varchar AS WLS_WOPMSCHEDULE, 
                        src:WLS_WOPRIORITY::varchar AS WLS_WOPRIORITY, 
                        src:WLS_WOPROJBUD::varchar AS WLS_WOPROJBUD, 
                        src:WLS_WOPROJECT::varchar AS WLS_WOPROJECT, 
                        src:WLS_WOREPORTEDBY::varchar AS WLS_WOREPORTEDBY, 
                        src:WLS_WORKORDER::varchar AS WLS_WORKORDER, 
                        src:WLS_WOSCHEDENDDATE::datetime AS WLS_WOSCHEDENDDATE, 
                        src:WLS_WOSCHEDSTARTDATE::datetime AS WLS_WOSCHEDSTARTDATE, 
                        src:WLS_WOSHIFT::varchar AS WLS_WOSHIFT, 
                        src:WLS_WOSTATUS::varchar AS WLS_WOSTATUS, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:WLS_CODE,
            src:WLS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WLS_CODE  ORDER BY 
            src:WLS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WOLABORSCHEDPARAMS as strm))