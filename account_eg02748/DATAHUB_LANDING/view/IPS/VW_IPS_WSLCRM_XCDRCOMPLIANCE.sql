CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XCDRCOMPLIANCE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CONTACTORZONE::varchar AS CONTACTORZONE, 
                        src:CONTRACTOR::varchar AS CONTRACTOR, 
                        src:CONTRACTORAREA::varchar AS CONTRACTORAREA, 
                        src:CONTRACTORCODE::varchar AS CONTRACTORCODE, 
                        src:CONTRACTORPRIORITY::varchar AS CONTRACTORPRIORITY, 
                        src:CONTRACTORREFERENCE::varchar AS CONTRACTORREFERENCE, 
                        src:CONTRACTORREFNO::varchar AS CONTRACTORREFNO, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NETWORKOPSHUB::varchar AS NETWORKOPSHUB, 
                        src:SERVICEAREA::varchar AS SERVICEAREA, 
                        src:SERVNO::integer AS SERVNO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XCDRCOMPLIANCEKEY::integer AS XCDRCOMPLIANCEKEY, 
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
    
                        
                src:XCDRCOMPLIANCEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XCDRCOMPLIANCEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XCDRCOMPLIANCE as strm))