CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PLANNING_PLANPLCONDTYPE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLANCONDSTATUSKEY::integer AS APPLANCONDSTATUSKEY, 
                        src:APPLANPLCONDINHERITKEY::integer AS APPLANPLCONDINHERITKEY, 
                        src:APPLANPLCONDMILESTONEKEY::integer AS APPLANPLCONDMILESTONEKEY, 
                        src:APPLANPLCONDTYPECATKEY::integer AS APPLANPLCONDTYPECATKEY, 
                        src:APPLANPLCONDTYPEKEY::integer AS APPLANPLCONDTYPEKEY, 
                        src:AUTOSTATUSFRMLKEY::integer AS AUTOSTATUSFRMLKEY, 
                        src:BLDGAUTOSTATUSFRMLKEY::integer AS BLDGAUTOSTATUSFRMLKEY, 
                        src:CONDTITLE::varchar AS CONDTITLE, 
                        src:CONDTYPE::integer AS CONDTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPARTMENT::varchar AS DEPARTMENT, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PLANAUTOSTATUSFRMLKEY::integer AS PLANAUTOSTATUSFRMLKEY, 
                        src:PROJAUTOSTATUSFRMLKEY::integer AS PROJAUTOSTATUSFRMLKEY, 
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
    
                        
                src:APPLANPLCONDTYPEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPLANPLCONDTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PLANNING_PLANPLCONDTYPE as strm))