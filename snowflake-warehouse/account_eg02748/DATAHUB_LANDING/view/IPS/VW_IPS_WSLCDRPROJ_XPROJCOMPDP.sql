CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XPROJCOMPDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJAPPLDTLKEY::integer AS APPROJAPPLDTLKEY, 
                        src:COAASBUILTATTACHED::varchar AS COAASBUILTATTACHED, 
                        src:CS3ATTACHED::varchar AS CS3ATTACHED, 
                        src:CS4ATTACHED::varchar AS CS4ATTACHED, 
                        src:CS4DAILYLOGATTACHED::varchar AS CS4DAILYLOGATTACHED, 
                        src:CS4SCHEDULEATTACHED::varchar AS CS4SCHEDULEATTACHED, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DRAFTASBUILTATTACHED::varchar AS DRAFTASBUILTATTACHED, 
                        src:DRAFTASBUILTINWALKOVER::varchar AS DRAFTASBUILTINWALKOVER, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REQCOA::varchar AS REQCOA, 
                        src:REQDATEWALKOVER::datetime AS REQDATEWALKOVER, 
                        src:REQWALKOVER::varchar AS REQWALKOVER, 
                        src:SCHEDULEOFCOSTATTACHED::varchar AS SCHEDULEOFCOSTATTACHED, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XPROJCOMPDPKEY::integer AS XPROJCOMPDPKEY, 
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
    
                        
                src:XPROJCOMPDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPROJCOMPDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XPROJCOMPDP as strm))