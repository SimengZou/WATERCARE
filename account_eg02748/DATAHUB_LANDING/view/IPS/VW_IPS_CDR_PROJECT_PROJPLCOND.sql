CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PROJECT_PROJPLCOND AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLANSTATUSKEY::integer AS APPLANSTATUSKEY, 
                        src:APPROJKEY::integer AS APPROJKEY, 
                        src:APPROJPLCONDKEY::integer AS APPROJPLCONDKEY, 
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
    
                        
                src:APPROJPLCONDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPROJPLCONDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PROJECT_PROJPLCOND as strm))