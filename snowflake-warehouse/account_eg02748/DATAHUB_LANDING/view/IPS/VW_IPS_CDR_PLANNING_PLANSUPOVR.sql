CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PLANNING_PLANSUPOVR AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBILLACCTCHNGLOGKEY::integer AS APBILLACCTCHNGLOGKEY, 
                        src:APFEEKEY::integer AS APFEEKEY, 
                        src:APPAYKEY::integer AS APPAYKEY, 
                        src:APPLANCONDKEY::integer AS APPLANCONDKEY, 
                        src:APPLANDEFNKEY::integer AS APPLANDEFNKEY, 
                        src:APPLANKEY::integer AS APPLANKEY, 
                        src:APPLANPLCONDKEY::integer AS APPLANPLCONDKEY, 
                        src:APPLANSUPOVRKEY::integer AS APPLANSUPOVRKEY, 
                        src:APRFNDTRNNO::integer AS APRFNDTRNNO, 
                        src:BILLPAYKEY::integer AS BILLPAYKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MSFROMKEY::integer AS MSFROMKEY, 
                        src:MSTOKEY::integer AS MSTOKEY, 
                        src:SUPERVISOR::integer AS SUPERVISOR, 
                        src:SUPERVISOREMPLOYEE::varchar AS SUPERVISOREMPLOYEE, 
                        src:SUPOVRTYPE::integer AS SUPOVRTYPE, 
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
    
                        
                src:APPLANSUPOVRKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPLANSUPOVRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PLANNING_PLANSUPOVR as strm))