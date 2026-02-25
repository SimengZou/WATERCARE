CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_IDENTBANK AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BANK::varchar AS BANK, 
                        src:BANKACCOUNTNUMBER::varchar AS BANKACCOUNTNUMBER, 
                        src:BANKACCOUNTTYPE::varchar AS BANKACCOUNTTYPE, 
                        src:BANKADDRESS::varchar AS BANKADDRESS, 
                        src:BANKBRANCH::varchar AS BANKBRANCH, 
                        src:BANKCITY::varchar AS BANKCITY, 
                        src:BANKPHONE::varchar AS BANKPHONE, 
                        src:BANKROUTINGNUMBER::varchar AS BANKROUTINGNUMBER, 
                        src:BANKSTATE::varchar AS BANKSTATE, 
                        src:BANKZIP::varchar AS BANKZIP, 
                        src:CARDEXPIREDATE::datetime AS CARDEXPIREDATE, 
                        src:CARDNUMBER::varchar AS CARDNUMBER, 
                        src:CARDSECURITYNUMBER::varchar AS CARDSECURITYNUMBER, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIRECTDEBITSTATUS::varchar AS DIRECTDEBITSTATUS, 
                        src:DIRECTDEBITTRANSMITDATE::datetime AS DIRECTDEBITTRANSMITDATE, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:IDENTBANKKEY::integer AS IDENTBANKKEY, 
                        src:IDENTITYKEY::integer AS IDENTITYKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NAMEONACCOUNT::varchar AS NAMEONACCOUNT, 
                        src:PRENOTEDDRUNKEY::integer AS PRENOTEDDRUNKEY, 
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
    
                        
                src:IDENTBANKKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:IDENTBANKKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_IDENTBANK as strm))