CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_IDENTITYALERT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALERTDEFINITIONKEY::integer AS ALERTDEFINITIONKEY, 
                        src:ALERTTYPE::varchar AS ALERTTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DELINQUENCYMSALERTKEY::integer AS DELINQUENCYMSALERTKEY, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:IDENTITYALERTKEY::integer AS IDENTITYALERTKEY, 
                        src:IDENTITYKEY::integer AS IDENTITYKEY, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REASON::varchar AS REASON, 
                        src:RESOLUTION::varchar AS RESOLUTION, 
                        src:RESOLVEDBY::varchar AS RESOLVEDBY, 
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
    
                        
                src:IDENTITYALERTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:IDENTITYALERTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_IDENTITYALERT as strm))