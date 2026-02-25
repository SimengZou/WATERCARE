CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR AS SELECT
                        src:ACTIVITYCODE::varchar AS ACTIVITYCODE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISCONNECTIONDATE::varchar AS DISCONNECTIONDATE, 
                        src:DISCONNECTIONTYPE::varchar AS DISCONNECTIONTYPE, 
                        src:IPSCOMPKEY::integer AS IPSCOMPKEY, 
                        src:LEAKCUSTOMERSIDE::varchar AS LEAKCUSTOMERSIDE, 
                        src:LEAKWATERCARESIDE::varchar AS LEAKWATERCARESIDE, 
                        src:LOCATIONDESCRIPTION::varchar AS LOCATIONDESCRIPTION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEWMETERID::varchar AS NEWMETERID, 
                        src:NEWMETERINSTALLDATE::varchar AS NEWMETERINSTALLDATE, 
                        src:NEWMETERREADING::varchar AS NEWMETERREADING, 
                        src:OLDMETERID::varchar AS OLDMETERID, 
                        src:OLDMETERREADING::varchar AS OLDMETERREADING, 
                        src:ORIGINATEDSR::integer AS ORIGINATEDSR, 
                        src:SRCREATED::varchar AS SRCREATED, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKORDER::varchar AS WORKORDER, 
                        src:XFAULTBILLINGAUTOSRKEY::integer AS XFAULTBILLINGAUTOSRKEY, 
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
    
                        
                src:XFAULTBILLINGAUTOSRKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XFAULTBILLINGAUTOSRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR as strm))