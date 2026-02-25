CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTCHECKLISTS AS SELECT
                        src:ACK_ACT::numeric(38, 10) AS ACK_ACT, 
                        src:ACK_ADJUSTED::varchar AS ACK_ADJUSTED, 
                        src:ACK_ASMLEVEL::varchar AS ACK_ASMLEVEL, 
                        src:ACK_ASPECT::varchar AS ACK_ASPECT, 
                        src:ACK_CASETASK::varchar AS ACK_CASETASK, 
                        src:ACK_CODE::varchar AS ACK_CODE, 
                        src:ACK_COLOR::varchar AS ACK_COLOR, 
                        src:ACK_COMPLETED::varchar AS ACK_COMPLETED, 
                        src:ACK_COMPLEVEL::varchar AS ACK_COMPLEVEL, 
                        src:ACK_COMPLOCATION::varchar AS ACK_COMPLOCATION, 
                        src:ACK_CONDITION::varchar AS ACK_CONDITION, 
                        src:ACK_CONDITION_AS_FOUND::varchar AS ACK_CONDITION_AS_FOUND, 
                        src:ACK_DEFMAINT::varchar AS ACK_DEFMAINT, 
                        src:ACK_DESC::varchar AS ACK_DESC, 
                        src:ACK_DIRECTION::varchar AS ACK_DIRECTION, 
                        src:ACK_ENABLENONCONFORMITIES::varchar AS ACK_ENABLENONCONFORMITIES, 
                        src:ACK_ENTITYKEY::varchar AS ACK_ENTITYKEY, 
                        src:ACK_ENTITYORG::varchar AS ACK_ENTITYORG, 
                        src:ACK_EVENT::varchar AS ACK_EVENT, 
                        src:ACK_FINALOCCURRENCE::varchar AS ACK_FINALOCCURRENCE, 
                        src:ACK_FINDING::varchar AS ACK_FINDING, 
                        src:ACK_FOLLOWUP::varchar AS ACK_FOLLOWUP, 
                        src:ACK_FOLLOWUPACT::numeric(38, 10) AS ACK_FOLLOWUPACT, 
                        src:ACK_FOLLOWUPEVENT::varchar AS ACK_FOLLOWUPEVENT, 
                        src:ACK_FORMULA::varchar AS ACK_FORMULA, 
                        src:ACK_FROMGEOREF::varchar AS ACK_FROMGEOREF, 
                        src:ACK_FROMJOBPLAN::varchar AS ACK_FROMJOBPLAN, 
                        src:ACK_FROMJOBPLANREV::numeric(38, 10) AS ACK_FROMJOBPLANREV, 
                        src:ACK_FROMPOINT::numeric(38, 10) AS ACK_FROMPOINT, 
                        src:ACK_FROMREFDESC::varchar AS ACK_FROMREFDESC, 
                        src:ACK_FROM_HORIZONTAL_OFFSET::numeric(38, 10) AS ACK_FROM_HORIZONTAL_OFFSET, 
                        src:ACK_FROM_HORIZONTAL_OFFSETTYPE::varchar AS ACK_FROM_HORIZONTAL_OFFSETTYPE, 
                        src:ACK_FROM_HORIZONTAL_OFFSETUOM::varchar AS ACK_FROM_HORIZONTAL_OFFSETUOM, 
                        src:ACK_FROM_LATITUDE::numeric(38, 10) AS ACK_FROM_LATITUDE, 
                        src:ACK_FROM_LONGITUDE::numeric(38, 10) AS ACK_FROM_LONGITUDE, 
                        src:ACK_FROM_OFFSET::numeric(38, 10) AS ACK_FROM_OFFSET, 
                        src:ACK_FROM_OFFSET_DIRECTION::varchar AS ACK_FROM_OFFSET_DIRECTION, 
                        src:ACK_FROM_OFFSET_PERCENTAGE::numeric(38, 10) AS ACK_FROM_OFFSET_PERCENTAGE, 
                        src:ACK_FROM_REFERENCE::numeric(38, 10) AS ACK_FROM_REFERENCE, 
                        src:ACK_FROM_RELATIONSHIP_TYPE::varchar AS ACK_FROM_RELATIONSHIP_TYPE, 
                        src:ACK_FROM_VERTICAL_OFFSET::numeric(38, 10) AS ACK_FROM_VERTICAL_OFFSET, 
                        src:ACK_FROM_VERTICAL_OFFSETTYPE::varchar AS ACK_FROM_VERTICAL_OFFSETTYPE, 
                        src:ACK_FROM_VERTICAL_OFFSETUOM::varchar AS ACK_FROM_VERTICAL_OFFSETUOM, 
                        src:ACK_FROM_XCOORDINATE::numeric(38, 10) AS ACK_FROM_XCOORDINATE, 
                        src:ACK_FROM_YCOORDINATE::numeric(38, 10) AS ACK_FROM_YCOORDINATE, 
                        src:ACK_GOOD::varchar AS ACK_GOOD, 
                        src:ACK_GROUP_LABEL::varchar AS ACK_GROUP_LABEL, 
                        src:ACK_HIDEFOLLOWUP::varchar AS ACK_HIDEFOLLOWUP, 
                        src:ACK_HIDELINEARFIELDS::varchar AS ACK_HIDELINEARFIELDS, 
                        src:ACK_JOB::varchar AS ACK_JOB, 
                        src:ACK_LASTSAVED::datetime AS ACK_LASTSAVED, 
                        src:ACK_LOTO::varchar AS ACK_LOTO, 
                        src:ACK_MATLIST::varchar AS ACK_MATLIST, 
                        src:ACK_MAXSLIDERVALUE::numeric(38, 10) AS ACK_MAXSLIDERVALUE, 
                        src:ACK_MEASUREMENTRESP::varchar AS ACK_MEASUREMENTRESP, 
                        src:ACK_METRICFRACTIONSLIDER::varchar AS ACK_METRICFRACTIONSLIDER, 
                        src:ACK_MINSLIDERVALUE::numeric(38, 10) AS ACK_MINSLIDERVALUE, 
                        src:ACK_MOBILEDATEUPDATED::datetime AS ACK_MOBILEDATEUPDATED, 
                        src:ACK_NO::varchar AS ACK_NO, 
                        src:ACK_NONCONFORMITY::varchar AS ACK_NONCONFORMITY, 
                        src:ACK_NONCONFORMITYFOUND::varchar AS ACK_NONCONFORMITYFOUND, 
                        src:ACK_NONCONFORMITYTYPE::varchar AS ACK_NONCONFORMITYTYPE, 
                        src:ACK_NONCONFORMITYTYPE_ORG::varchar AS ACK_NONCONFORMITYTYPE_ORG, 
                        src:ACK_NOTES::varchar AS ACK_NOTES, 
                        src:ACK_NOT_APPLICABLE::varchar AS ACK_NOT_APPLICABLE, 
                        src:ACK_OBJECT::varchar AS ACK_OBJECT, 
                        src:ACK_OBJECTLEVEL::varchar AS ACK_OBJECTLEVEL, 
                        src:ACK_OBJECT_ORG::varchar AS ACK_OBJECT_ORG, 
                        src:ACK_OCCURRENCE::numeric(38, 10) AS ACK_OCCURRENCE, 
                        src:ACK_OK::varchar AS ACK_OK, 
                        src:ACK_PARENTKEY::varchar AS ACK_PARENTKEY, 
                        src:ACK_PART::varchar AS ACK_PART, 
                        src:ACK_PARTCONDITION::varchar AS ACK_PARTCONDITION, 
                        src:ACK_PART_ORG::varchar AS ACK_PART_ORG, 
                        src:ACK_POINT::varchar AS ACK_POINT, 
                        src:ACK_POINTTYPE::varchar AS ACK_POINTTYPE, 
                        src:ACK_POOR::varchar AS ACK_POOR, 
                        src:ACK_POSSIBLEFINDINGS::varchar AS ACK_POSSIBLEFINDINGS, 
                        src:ACK_POSSIBLE_CONDS_AS_FOUND::varchar AS ACK_POSSIBLE_CONDS_AS_FOUND, 
                        src:ACK_POSSIBLE_DIRECTIONS::varchar AS ACK_POSSIBLE_DIRECTIONS, 
                        src:ACK_POSSIBLE_NA_OPTIONS::varchar AS ACK_POSSIBLE_NA_OPTIONS, 
                        src:ACK_POSSIBLE_SEVERITIES::varchar AS ACK_POSSIBLE_SEVERITIES, 
                        src:ACK_READING::varchar AS ACK_READING, 
                        src:ACK_REFERENCE::varchar AS ACK_REFERENCE, 
                        src:ACK_RELATIONSHIP_TYPE::varchar AS ACK_RELATIONSHIP_TYPE, 
                        src:ACK_RENTITY::varchar AS ACK_RENTITY, 
                        src:ACK_REPAIRSNEEDED::varchar AS ACK_REPAIRSNEEDED, 
                        src:ACK_REQUIREDTOCLOSE::varchar AS ACK_REQUIREDTOCLOSE, 
                        src:ACK_RESOLUTION::varchar AS ACK_RESOLUTION, 
                        src:ACK_RESPONSIBILITY::varchar AS ACK_RESPONSIBILITY, 
                        src:ACK_ROUTESEQUENCE::numeric(38, 10) AS ACK_ROUTESEQUENCE, 
                        src:ACK_SEQUENCE::numeric(38, 10) AS ACK_SEQUENCE, 
                        src:ACK_SEVERITY::varchar AS ACK_SEVERITY, 
                        src:ACK_SRCCALCULATEDVALUE::varchar AS ACK_SRCCALCULATEDVALUE, 
                        src:ACK_SYSLEVEL::varchar AS ACK_SYSLEVEL, 
                        src:ACK_TASK::varchar AS ACK_TASK, 
                        src:ACK_TASKCHECKLISTCODE::varchar AS ACK_TASKCHECKLISTCODE, 
                        src:ACK_TOGEOREF::varchar AS ACK_TOGEOREF, 
                        src:ACK_TOLERANCECRNONCONFORMITY::varchar AS ACK_TOLERANCECRNONCONFORMITY, 
                        src:ACK_TOLERANCESEVERITY::varchar AS ACK_TOLERANCESEVERITY, 
                        src:ACK_TOLERANCE_NCF_TYPE::varchar AS ACK_TOLERANCE_NCF_TYPE, 
                        src:ACK_TOLERANCE_NCF_TYPE_ORG::varchar AS ACK_TOLERANCE_NCF_TYPE_ORG, 
                        src:ACK_TOPOINT::numeric(38, 10) AS ACK_TOPOINT, 
                        src:ACK_TOREFDESC::varchar AS ACK_TOREFDESC, 
                        src:ACK_TO_HORIZONTAL_OFFSET::numeric(38, 10) AS ACK_TO_HORIZONTAL_OFFSET, 
                        src:ACK_TO_HORIZONTAL_OFFSETTYPE::varchar AS ACK_TO_HORIZONTAL_OFFSETTYPE, 
                        src:ACK_TO_HORIZONTAL_OFFSETUOM::varchar AS ACK_TO_HORIZONTAL_OFFSETUOM, 
                        src:ACK_TO_LATITUDE::numeric(38, 10) AS ACK_TO_LATITUDE, 
                        src:ACK_TO_LONGITUDE::numeric(38, 10) AS ACK_TO_LONGITUDE, 
                        src:ACK_TO_OFFSET::numeric(38, 10) AS ACK_TO_OFFSET, 
                        src:ACK_TO_OFFSET_DIRECTION::varchar AS ACK_TO_OFFSET_DIRECTION, 
                        src:ACK_TO_OFFSET_PERCENTAGE::numeric(38, 10) AS ACK_TO_OFFSET_PERCENTAGE, 
                        src:ACK_TO_REFERENCE::numeric(38, 10) AS ACK_TO_REFERENCE, 
                        src:ACK_TO_RELATIONSHIP_TYPE::varchar AS ACK_TO_RELATIONSHIP_TYPE, 
                        src:ACK_TO_VERTICAL_OFFSET::numeric(38, 10) AS ACK_TO_VERTICAL_OFFSET, 
                        src:ACK_TO_VERTICAL_OFFSETTYPE::varchar AS ACK_TO_VERTICAL_OFFSETTYPE, 
                        src:ACK_TO_VERTICAL_OFFSETUOM::varchar AS ACK_TO_VERTICAL_OFFSETUOM, 
                        src:ACK_TO_XCOORDINATE::numeric(38, 10) AS ACK_TO_XCOORDINATE, 
                        src:ACK_TO_YCOORDINATE::numeric(38, 10) AS ACK_TO_YCOORDINATE, 
                        src:ACK_TYPE::varchar AS ACK_TYPE, 
                        src:ACK_UOM::varchar AS ACK_UOM, 
                        src:ACK_UPDATECOUNT::numeric(38, 10) AS ACK_UPDATECOUNT, 
                        src:ACK_UPDATED::datetime AS ACK_UPDATED, 
                        src:ACK_UPDATEDBY::varchar AS ACK_UPDATEDBY, 
                        src:ACK_VALUE::numeric(38, 10) AS ACK_VALUE, 
                        src:ACK_YES::varchar AS ACK_YES, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ACK_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
                                        
                src:ACK_CODE  ORDER BY 
            src:ACK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACTCHECKLISTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_ACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FOLLOWUPACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROMJOBPLANREV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROMPOINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_REFERENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_VERTICAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_FROM_YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACK_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_MAXSLIDERVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_MINSLIDERVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACK_MOBILEDATEUPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_OCCURRENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_ROUTESEQUENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_SEQUENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TOPOINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_REFERENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_VERTICAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_TO_YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACK_UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACK_VALUE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACK_LASTSAVED), '1900-01-01')) is not null