CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XPLANNEDAUDITGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHANGEDBY::varchar AS CHANGEDBY, 
                        src:CHANGEDDATETIME::datetime AS CHANGEDDATETIME, 
                        src:CHANGEDVALUE::varchar AS CHANGEDVALUE, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORIGINALVALUE::varchar AS ORIGINALVALUE, 
                        src:TYPE::varchar AS TYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBILLINGDATADPKEY::integer AS XBILLINGDATADPKEY, 
                        src:XPLANNEDAUDITGDKEY::integer AS XPLANNEDAUDITGDKEY, 
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
    
                        
                src:XPLANNEDAUDITGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPLANNEDAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XPLANNEDAUDITGD as strm))