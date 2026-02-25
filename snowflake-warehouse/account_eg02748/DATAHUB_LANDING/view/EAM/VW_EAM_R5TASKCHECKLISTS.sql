CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5TASKCHECKLISTS AS SELECT
                        src:TCH_ASMLEVEL::varchar AS TCH_ASMLEVEL, 
                        src:TCH_ASPECT::varchar AS TCH_ASPECT, 
                        src:TCH_CODE::varchar AS TCH_CODE, 
                        src:TCH_COLOR::varchar AS TCH_COLOR, 
                        src:TCH_COMPLEVEL::varchar AS TCH_COMPLEVEL, 
                        src:TCH_COMPLOCATION::varchar AS TCH_COMPLOCATION, 
                        src:TCH_CONDITION::varchar AS TCH_CONDITION, 
                        src:TCH_CONDITION_OPTIONS::varchar AS TCH_CONDITION_OPTIONS, 
                        src:TCH_DESC::varchar AS TCH_DESC, 
                        src:TCH_DIRECTION_OPTIONS::varchar AS TCH_DIRECTION_OPTIONS, 
                        src:TCH_ENABLENONCONFORMITIES::varchar AS TCH_ENABLENONCONFORMITIES, 
                        src:TCH_EQUIPFILTER::varchar AS TCH_EQUIPFILTER, 
                        src:TCH_FOLLOWUPJOB::varchar AS TCH_FOLLOWUPJOB, 
                        src:TCH_FOLLOWUPTASK::varchar AS TCH_FOLLOWUPTASK, 
                        src:TCH_FORMULA::varchar AS TCH_FORMULA, 
                        src:TCH_FRACTIONSLIDERDIMENSIONS::varchar AS TCH_FRACTIONSLIDERDIMENSIONS, 
                        src:TCH_GROUP_LABEL::varchar AS TCH_GROUP_LABEL, 
                        src:TCH_HIDEFOLLOWUP::varchar AS TCH_HIDEFOLLOWUP, 
                        src:TCH_HIDELINEARFIELDS::varchar AS TCH_HIDELINEARFIELDS, 
                        src:TCH_LASTSAVED::datetime AS TCH_LASTSAVED, 
                        src:TCH_MATLIST::varchar AS TCH_MATLIST, 
                        src:TCH_MAXSLIDERVALUE::numeric(38, 10) AS TCH_MAXSLIDERVALUE, 
                        src:TCH_MEASUREMENTRESP::varchar AS TCH_MEASUREMENTRESP, 
                        src:TCH_METRICFRACTIONSLIDER::varchar AS TCH_METRICFRACTIONSLIDER, 
                        src:TCH_MINSLIDERVALUE::numeric(38, 10) AS TCH_MINSLIDERVALUE, 
                        src:TCH_NOTUSED::varchar AS TCH_NOTUSED, 
                        src:TCH_NOT_APPLICABLE_OPTIONS::varchar AS TCH_NOT_APPLICABLE_OPTIONS, 
                        src:TCH_OBJECTCATEGORY::varchar AS TCH_OBJECTCATEGORY, 
                        src:TCH_OBJECTCLASS::varchar AS TCH_OBJECTCLASS, 
                        src:TCH_OBJECTCLASS_ORG::varchar AS TCH_OBJECTCLASS_ORG, 
                        src:TCH_OBJECTLEVEL::varchar AS TCH_OBJECTLEVEL, 
                        src:TCH_PART::varchar AS TCH_PART, 
                        src:TCH_PARTCATEGORY::varchar AS TCH_PARTCATEGORY, 
                        src:TCH_PARTCLASS::varchar AS TCH_PARTCLASS, 
                        src:TCH_PARTCLASS_ORG::varchar AS TCH_PARTCLASS_ORG, 
                        src:TCH_PARTCONDITION::varchar AS TCH_PARTCONDITION, 
                        src:TCH_PART_ORG::varchar AS TCH_PART_ORG, 
                        src:TCH_POINTTYPE::varchar AS TCH_POINTTYPE, 
                        src:TCH_POSSIBLEFINDINGS::varchar AS TCH_POSSIBLEFINDINGS, 
                        src:TCH_REFERENCE::varchar AS TCH_REFERENCE, 
                        src:TCH_REPEATING::varchar AS TCH_REPEATING, 
                        src:TCH_REQUIREDTOCLOSE::varchar AS TCH_REQUIREDTOCLOSE, 
                        src:TCH_RESPONSIBILITY::varchar AS TCH_RESPONSIBILITY, 
                        src:TCH_SEQUENCE::numeric(38, 10) AS TCH_SEQUENCE, 
                        src:TCH_SRCCALCULATEDVALUE::varchar AS TCH_SRCCALCULATEDVALUE, 
                        src:TCH_SYSLEVEL::varchar AS TCH_SYSLEVEL, 
                        src:TCH_TASK::varchar AS TCH_TASK, 
                        src:TCH_TASKREV::numeric(38, 10) AS TCH_TASKREV, 
                        src:TCH_TYPE::varchar AS TCH_TYPE, 
                        src:TCH_UOM::varchar AS TCH_UOM, 
                        src:TCH_UPDATECOUNT::numeric(38, 10) AS TCH_UPDATECOUNT, 
                        src:TCH_UPDATED::datetime AS TCH_UPDATED, 
                        src:TCH_UPDATEDBY::varchar AS TCH_UPDATEDBY, 
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
    
                        
                src:TCH_CODE,
            src:TCH_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TCH_CODE  ORDER BY 
            src:TCH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5TASKCHECKLISTS as strm))