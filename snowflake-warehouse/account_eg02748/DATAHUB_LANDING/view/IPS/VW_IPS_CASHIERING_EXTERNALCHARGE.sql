CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_EXTERNALCHARGE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHARGEDETAILS::varchar AS CHARGEDETAILS, 
                        src:CHARGESETUP::varchar AS CHARGESETUP, 
                        src:CHGSUBTOT::numeric(38, 10) AS CHGSUBTOT, 
                        src:CHGTAX::numeric(38, 10) AS CHGTAX, 
                        src:CHGTAXRATE::numeric(38, 10) AS CHGTAXRATE, 
                        src:CHGTOT::numeric(38, 10) AS CHGTOT, 
                        src:CHGUNITPRC::numeric(38, 10) AS CHGUNITPRC, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXTCHGKEY::integer AS EXTCHGKEY, 
                        src:EXTERNALKEY::integer AS EXTERNALKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAID::varchar AS PAID, 
                        src:PAYMENTAMOUNT::numeric(38, 10) AS PAYMENTAMOUNT, 
                        src:TENDERTYPE::integer AS TENDERTYPE, 
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
    
                        
                src:EXTCHGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EXTCHGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_EXTERNALCHARGE as strm))