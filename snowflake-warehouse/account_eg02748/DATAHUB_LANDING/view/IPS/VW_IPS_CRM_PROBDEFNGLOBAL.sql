CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CRM_PROBDEFNGLOBAL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHECKDUPLICATELOCALONLY::varchar AS CHECKDUPLICATELOCALONLY, 
                        src:CHKDUPFLG::varchar AS CHKDUPFLG, 
                        src:CORRPROCESSSETUP::integer AS CORRPROCESSSETUP, 
                        src:CREATEDINLASTXMIN::integer AS CREATEDINLASTXMIN, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTIFYCUSTOMER::varchar AS NOTIFYCUSTOMER, 
                        src:PROBDEFNGLOBALKEY::integer AS PROBDEFNGLOBALKEY, 
                        src:RESOLVELINKEDREQUESTS::varchar AS RESOLVELINKEDREQUESTS, 
                        src:SAMEREQUESTTYPEFLG::varchar AS SAMEREQUESTTYPEFLG, 
                        src:SEARCHRADIUS::numeric(38, 10) AS SEARCHRADIUS, 
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
    
                        
                src:PROBDEFNGLOBALKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PROBDEFNGLOBALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CRM_PROBDEFNGLOBAL as strm))