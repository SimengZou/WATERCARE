CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_BUILDING_BLDGREVIEW AS SELECT
                        src:ACTLTM::numeric(38, 10) AS ACTLTM, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGKEY::integer AS APBLDGKEY, 
                        src:APBLDGREVIEWKEY::integer AS APBLDGREVIEWKEY, 
                        src:APBLDGREVIEWTYPEKEY::integer AS APBLDGREVIEWTYPEKEY, 
                        src:ASSIGNED::varchar AS ASSIGNED, 
                        src:ASSIGNTO::varchar AS ASSIGNTO, 
                        src:ASSIGNTOPROVIDER::integer AS ASSIGNTOPROVIDER, 
                        src:CMPTRGEN::varchar AS CMPTRGEN, 
                        src:COMP::varchar AS COMP, 
                        src:COMPBY::varchar AS COMPBY, 
                        src:COMPBYPROVIDER::integer AS COMPBYPROVIDER, 
                        src:COMPDTTM::datetime AS COMPDTTM, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPT::varchar AS DEPT, 
                        src:ISSBY::varchar AS ISSBY, 
                        src:ISSDTTM::datetime AS ISSDTTM, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RELEVANTREVIEW::varchar AS RELEVANTREVIEW, 
                        src:RESULT::varchar AS RESULT, 
                        src:RESULTBY::varchar AS RESULTBY, 
                        src:RESULTBYPROVIDER::integer AS RESULTBYPROVIDER, 
                        src:RESULTDTTM::datetime AS RESULTDTTM, 
                        src:RESULTGEN::varchar AS RESULTGEN, 
                        src:SCHEDBY::varchar AS SCHEDBY, 
                        src:SCHEDDTTM::datetime AS SCHEDDTTM, 
                        src:STARTBY::varchar AS STARTBY, 
                        src:STARTBYPROVIDER::integer AS STARTBYPROVIDER, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STARTED::varchar AS STARTED, 
                        src:SUSPDT::datetime AS SUSPDT, 
                        src:TYPENO::integer AS TYPENO, 
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
    
                        
                src:APBLDGREVIEWKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APBLDGREVIEWKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_BUILDING_BLDGREVIEW as strm))