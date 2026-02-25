CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XVALUATIONSAUDITGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSETID::varchar AS ASSETID, 
                        src:ASSETKEY::integer AS ASSETKEY, 
                        src:CHANGEDDATE::datetime AS CHANGEDDATE, 
                        src:CHANGEDVALUE::varchar AS CHANGEDVALUE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:ENTITYKEY::integer AS ENTITYKEY, 
                        src:ENTITYNAME::varchar AS ENTITYNAME, 
                        src:FIELDNAME::varchar AS FIELDNAME, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODIFIEDBYEMPLOYEE::varchar AS MODIFIEDBYEMPLOYEE, 
                        src:ORIGINALVALUE::varchar AS ORIGINALVALUE, 
                        src:VALUATIONKEY::integer AS VALUATIONKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XVALUATIONSAUDITGDKEY::integer AS XVALUATIONSAUDITGDKEY, 
                        src:XVALUATIONSDPKEY::integer AS XVALUATIONSDPKEY, 
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
    
                        
                src:XVALUATIONSAUDITGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XVALUATIONSAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XVALUATIONSAUDITGD as strm))