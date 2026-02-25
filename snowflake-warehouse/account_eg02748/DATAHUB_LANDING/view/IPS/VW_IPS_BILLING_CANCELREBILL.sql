CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_CANCELREBILL AS SELECT
                        src:ACCOUNTAREA::varchar AS ACCOUNTAREA, 
                        src:ACCOUNTCLASS::varchar AS ACCOUNTCLASS, 
                        src:ACCOUNTGROUP::varchar AS ACCOUNTGROUP, 
                        src:ACCOUNTSUBGROUP::varchar AS ACCOUNTSUBGROUP, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLINGCYCLE::varchar AS BILLINGCYCLE, 
                        src:BILLINGPERIODSTARTDATE::datetime AS BILLINGPERIODSTARTDATE, 
                        src:BILLINGPERIODSTOPDATE::datetime AS BILLINGPERIODSTOPDATE, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:CANCELREBILLKEY::integer AS CANCELREBILLKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBERBILLS::integer AS NUMBERBILLS, 
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
    
                        
                src:CANCELREBILLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CANCELREBILLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_CANCELREBILL as strm))