CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XRETAILAUDITGD AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSETKEY::integer AS ASSETKEY, 
                        src:BLUSAGEDIFF::varchar AS BLUSAGEDIFF, 
                        src:DELETED::boolean AS DELETED, 
                        src:FIELDNAME::varchar AS FIELDNAME, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODIFIEDBYEMPLOYEE::varchar AS MODIFIEDBYEMPLOYEE, 
                        src:MODIFIEDDATE::datetime AS MODIFIEDDATE, 
                        src:NEWBLUSAGE::varchar AS NEWBLUSAGE, 
                        src:NEWVALUE::varchar AS NEWVALUE, 
                        src:PREVIOUSBLUSAGE::varchar AS PREVIOUSBLUSAGE, 
                        src:PREVIOUSVALUE::varchar AS PREVIOUSVALUE, 
                        src:READINGDATE::datetime AS READINGDATE, 
                        src:READINGKEY::integer AS READINGKEY, 
                        src:SOURCEID::varchar AS SOURCEID, 
                        src:VALDIFF::varchar AS VALDIFF, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XRETAILAUDITDPKEY::integer AS XRETAILAUDITDPKEY, 
                        src:XRETAILAUDITGDKEY::integer AS XRETAILAUDITGDKEY, 
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
    
                        
                src:XRETAILAUDITGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XRETAILAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XRETAILAUDITGD as strm))