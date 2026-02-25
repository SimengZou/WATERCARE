CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_ESCROWPAYMENT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CARDAUTH::varchar AS CARDAUTH, 
                        src:CARDBANK::varchar AS CARDBANK, 
                        src:CARDEXPDT::datetime AS CARDEXPDT, 
                        src:CARDNAME::varchar AS CARDNAME, 
                        src:CARDNO::varchar AS CARDNO, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:CHECKBANK::varchar AS CHECKBANK, 
                        src:CHECKID::varchar AS CHECKID, 
                        src:CHECKNAME::varchar AS CHECKNAME, 
                        src:CHECKNO::varchar AS CHECKNO, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESCROWPAYKEY::integer AS ESCROWPAYKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYAMT::numeric(38, 10) AS PAYAMT, 
                        src:PAYDTTM::datetime AS PAYDTTM, 
                        src:PAYMETHOD::integer AS PAYMETHOD, 
                        src:REGTRNNO::integer AS REGTRNNO, 
                        src:TAKENBY::varchar AS TAKENBY, 
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
    
                        
                src:ESCROWPAYKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ESCROWPAYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_ESCROWPAYMENT as strm))