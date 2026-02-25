CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCTNOGENSEG AS SELECT
                        src:ACCTNOGENSEGKEY::integer AS ACCTNOGENSEGKEY, 
                        src:ACCTNOGENSETUPKEY::integer AS ACCTNOGENSETUPKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BUSOBJMON::varchar AS BUSOBJMON, 
                        src:COUNTOWNERSEPARATEFLAG::varchar AS COUNTOWNERSEPARATEFLAG, 
                        src:CURRENTVALUE::integer AS CURRENTVALUE, 
                        src:CUSTOMVALUE::varchar AS CUSTOMVALUE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPLAYOWNERAS::integer AS DISPLAYOWNERAS, 
                        src:DISPLAYOWNERASFLAG::varchar AS DISPLAYOWNERASFLAG, 
                        src:LEADINGZEROFLAG::varchar AS LEADINGZEROFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NODIGITS::integer AS NODIGITS, 
                        src:OWNERINDICATOR::varchar AS OWNERINDICATOR, 
                        src:SEGMENTNUMBER::integer AS SEGMENTNUMBER, 
                        src:SEGMENTTYPE::varchar AS SEGMENTTYPE, 
                        src:SEPARATOR::varchar AS SEPARATOR, 
                        src:STARTAT::integer AS STARTAT, 
                        src:TENANTINDICATOR::varchar AS TENANTINDICATOR, 
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
    
                        
                src:ACCTNOGENSEGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCTNOGENSEGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCTNOGENSEG as strm))