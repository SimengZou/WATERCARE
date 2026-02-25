CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_PAYMENTDISTRIBUTION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHGKEY::integer AS CHGKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISTAMT::numeric(38, 10) AS DISTAMT, 
                        src:DRWRTRANNO::integer AS DRWRTRANNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYDISTKEY::integer AS PAYDISTKEY, 
                        src:PAYSERVERTRANKEY::integer AS PAYSERVERTRANKEY, 
                        src:REFPD::integer AS REFPD, 
                        src:REGTRANNO::integer AS REGTRANNO, 
                        src:STATUS::varchar AS STATUS, 
                        src:TRANTYPE::varchar AS TRANTYPE, 
                        src:TRNNO::integer AS TRNNO, 
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
    
                        
                src:PAYDISTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PAYDISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_PAYMENTDISTRIBUTION as strm))