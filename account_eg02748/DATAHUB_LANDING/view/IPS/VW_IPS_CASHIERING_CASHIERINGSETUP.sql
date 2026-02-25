CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_CASHIERINGSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILL1::numeric(38, 10) AS BILL1, 
                        src:BILL2::numeric(38, 10) AS BILL2, 
                        src:BILL3::numeric(38, 10) AS BILL3, 
                        src:BILL4::numeric(38, 10) AS BILL4, 
                        src:BILL5::numeric(38, 10) AS BILL5, 
                        src:BILL6::numeric(38, 10) AS BILL6, 
                        src:BILL7::numeric(38, 10) AS BILL7, 
                        src:BILL8::numeric(38, 10) AS BILL8, 
                        src:CASHBGTNO::integer AS CASHBGTNO, 
                        src:CASHSUKEY::integer AS CASHSUKEY, 
                        src:COIN1::numeric(38, 10) AS COIN1, 
                        src:COIN2::numeric(38, 10) AS COIN2, 
                        src:COIN3::numeric(38, 10) AS COIN3, 
                        src:COIN4::numeric(38, 10) AS COIN4, 
                        src:COIN5::numeric(38, 10) AS COIN5, 
                        src:COIN6::numeric(38, 10) AS COIN6, 
                        src:COIN7::numeric(38, 10) AS COIN7, 
                        src:COIN8::numeric(38, 10) AS COIN8, 
                        src:COINROLL1::numeric(38, 10) AS COINROLL1, 
                        src:COINROLL2::numeric(38, 10) AS COINROLL2, 
                        src:COINROLL3::numeric(38, 10) AS COINROLL3, 
                        src:COINROLL4::numeric(38, 10) AS COINROLL4, 
                        src:COINROLL5::numeric(38, 10) AS COINROLL5, 
                        src:COINROLL6::numeric(38, 10) AS COINROLL6, 
                        src:COINROLL7::numeric(38, 10) AS COINROLL7, 
                        src:COINROLL8::numeric(38, 10) AS COINROLL8, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DRWRSTYLE::varchar AS DRWRSTYLE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RECEIPTFRMLAKEY::integer AS RECEIPTFRMLAKEY, 
                        src:TAXRATE::numeric(38, 10) AS TAXRATE, 
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
    
                        
                src:CASHSUKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CASHSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_CASHIERINGSETUP as strm))