CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PLANNING_PLANHEARING AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLICATIONKEY::integer AS APPLICATIONKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:COMPDTTM::datetime AS COMPDTTM, 
                        src:COMPFLAG::varchar AS COMPFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:DOCKETNUM::varchar AS DOCKETNUM, 
                        src:HEARTYPEKEY::integer AS HEARTYPEKEY, 
                        src:LOCATION::varchar AS LOCATION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NAME::varchar AS NAME, 
                        src:PLANHEARKEY::integer AS PLANHEARKEY, 
                        src:RECORDBY::varchar AS RECORDBY, 
                        src:RESULT::varchar AS RESULT, 
                        src:SCHEDBY::varchar AS SCHEDBY, 
                        src:SCHEDDTTM::datetime AS SCHEDDTTM, 
                        src:SCHEDFLAG::varchar AS SCHEDFLAG, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
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
    
                        
                src:PLANHEARKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PLANHEARKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PLANNING_PLANHEARING as strm))