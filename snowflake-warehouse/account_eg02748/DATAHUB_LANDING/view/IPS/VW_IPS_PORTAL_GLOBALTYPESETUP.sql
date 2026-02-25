CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PORTAL_GLOBALTYPESETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALLOWADDRELATED::varchar AS ALLOWADDRELATED, 
                        src:ALLOWDELETERELATED::varchar AS ALLOWDELETERELATED, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:GLOBALTYPESETUPKEY::integer AS GLOBALTYPESETUPKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRODUCTFAMILY::varchar AS PRODUCTFAMILY, 
                        src:SHOWRELATEDBUILDING::varchar AS SHOWRELATEDBUILDING, 
                        src:SHOWRELATEDBUSINESSLICENSE::varchar AS SHOWRELATEDBUSINESSLICENSE, 
                        src:SHOWRELATEDCASE::varchar AS SHOWRELATEDCASE, 
                        src:SHOWRELATEDPLANNING::varchar AS SHOWRELATEDPLANNING, 
                        src:SHOWRELATEDPROJECT::varchar AS SHOWRELATEDPROJECT, 
                        src:SHOWRELATEDREQUEST::varchar AS SHOWRELATEDREQUEST, 
                        src:SHOWRELATEDTRADELICENSE::varchar AS SHOWRELATEDTRADELICENSE, 
                        src:SHOWRELATEDUSE::varchar AS SHOWRELATEDUSE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WIZARDPROJECTTYPE::integer AS WIZARDPROJECTTYPE, 
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
    
                        
                src:GLOBALTYPESETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:GLOBALTYPESETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PORTAL_GLOBALTYPESETUP as strm))