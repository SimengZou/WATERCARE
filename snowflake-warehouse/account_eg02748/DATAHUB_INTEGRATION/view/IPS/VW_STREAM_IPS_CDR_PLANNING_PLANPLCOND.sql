CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_PLANNING_PLANPLCOND AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLANKEY::integer AS APPLANKEY, 
                        src:APPLANPLCONDKEY::integer AS APPLANPLCONDKEY, 
                        src:APPLANSTATUSKEY::integer AS APPLANSTATUSKEY, 
                        src:AUTOSTATUSFRMKEY::integer AS AUTOSTATUSFRMKEY, 
                        src:BLDGAUTOSTATUSFRMKEY::integer AS BLDGAUTOSTATUSFRMKEY, 
                        src:CONDTEXTCHANGED::varchar AS CONDTEXTCHANGED, 
                        src:CONDTITLE::varchar AS CONDTITLE, 
                        src:CONDTYPE::integer AS CONDTYPE, 
                        src:DEFFERREDMILESTONE::integer AS DEFFERREDMILESTONE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEPARTMENT::varchar AS DEPARTMENT, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INHERITRULEKEY::integer AS INHERITRULEKEY, 
                        src:MILESTONEDATE::datetime AS MILESTONEDATE, 
                        src:MILESTONERULE::integer AS MILESTONERULE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORIGINAPKEY::integer AS ORIGINAPKEY, 
                        src:ORIGINCDRTYPE::integer AS ORIGINCDRTYPE, 
                        src:PARENTAPKEY::integer AS PARENTAPKEY, 
                        src:PARENTCDRTYPE::integer AS PARENTCDRTYPE, 
                        src:PLANAUTOSTATUSFRMKEY::integer AS PLANAUTOSTATUSFRMKEY, 
                        src:PLANCONDNO::integer AS PLANCONDNO, 
                        src:PLCONDTYPE::integer AS PLCONDTYPE, 
                        src:PROJAUTOSTATUSFRMKEY::integer AS PROJAUTOSTATUSFRMKEY, 
                        src:STATUSBY::varchar AS STATUSBY, 
                        src:STATUSDATE::datetime AS STATUSDATE, 
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
                                        
                src:APPLANPLCONDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANPLCOND as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLANPLCONDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLANSTATUSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTOSTATUSFRMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGAUTOSTATUSFRMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONDTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEFFERREDMILESTONE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INHERITRULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MILESTONEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MILESTONERULE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORIGINAPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORIGINCDRTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTAPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTCDRTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PLANAUTOSTATUSFRMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PLANCONDNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PLCONDTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROJAUTOSTATUSFRMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STATUSDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null