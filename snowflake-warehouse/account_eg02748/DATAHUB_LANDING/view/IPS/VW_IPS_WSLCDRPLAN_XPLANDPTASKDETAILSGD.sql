CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPLAN_XPLANDPTASKDETAILSGD AS SELECT
                        src:ACTIONID::integer AS ACTIONID, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMPLETEDDATE::datetime AS COMPLETEDDATE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DETAILS::varchar AS DETAILS, 
                        src:DUEDATE::datetime AS DUEDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTES::varchar AS NOTES, 
                        src:REVIEWCODE::varchar AS REVIEWCODE, 
                        src:REVIEWID::varchar AS REVIEWID, 
                        src:SCHEDULEDDATE::datetime AS SCHEDULEDDATE, 
                        src:TASKASSIGNEDT1::varchar AS TASKASSIGNEDT1, 
                        src:TASKASSIGNEDTO::varchar AS TASKASSIGNEDTO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XPLANDPACTIONLISTKEY::integer AS XPLANDPACTIONLISTKEY, 
                        src:XPLANDPTASKDETAILSGDKEY::integer AS XPLANDPTASKDETAILSGDKEY, 
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
    
                        
                src:XPLANDPTASKDETAILSGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPLANDPTASKDETAILSGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPLAN_XPLANDPTASKDETAILSGD as strm))