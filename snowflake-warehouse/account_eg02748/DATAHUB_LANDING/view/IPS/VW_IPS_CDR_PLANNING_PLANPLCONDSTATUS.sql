CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PLANNING_PLANPLCONDSTATUS AS SELECT
                        src:ACCESSID::integer AS ACCESSID, 
                        src:ACTIVE::varchar AS ACTIVE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLANPLCONDSTATUSKEY::integer AS APPLANPLCONDSTATUSKEY, 
                        src:CODE::varchar AS CODE, 
                        src:CONDMET::varchar AS CONDMET, 
                        src:CONDTEXTEDITABLE::varchar AS CONDTEXTEDITABLE, 
                        src:DEFERREDMILESTONE::varchar AS DEFERREDMILESTONE, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXCLUDEFROMINHERITANCE::varchar AS EXCLUDEFROMINHERITANCE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:IGNOREORDERONBLDG::varchar AS IGNOREORDERONBLDG, 
                        src:MARKASDELETED::varchar AS MARKASDELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
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
    
                        
                src:APPLANPLCONDSTATUSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPLANPLCONDSTATUSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PLANNING_PLANPLCONDSTATUS as strm))