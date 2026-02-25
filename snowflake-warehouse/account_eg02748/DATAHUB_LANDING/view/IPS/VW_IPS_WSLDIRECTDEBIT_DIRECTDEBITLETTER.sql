CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER AS SELECT
                        src:ACCOUNTDIRECTDEBITKEY::integer AS ACCOUNTDIRECTDEBITKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTNAME::varchar AS ACCOUNTNAME, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BANKACCTNO::varchar AS BANKACCTNO, 
                        src:BANKNAME::varchar AS BANKNAME, 
                        src:CCADDRESS::varchar AS CCADDRESS, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:DATEINITIATE::datetime AS DATEINITIATE, 
                        src:DATEOFCORRESPONDENCE::datetime AS DATEOFCORRESPONDENCE, 
                        src:DDFILENAME::varchar AS DDFILENAME, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIRECTDEBITLETTERKEY::integer AS DIRECTDEBITLETTERKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTADD1::varchar AS NOTADD1, 
                        src:NOTADD2::varchar AS NOTADD2, 
                        src:NOTADD3::varchar AS NOTADD3, 
                        src:NOTADD4::varchar AS NOTADD4, 
                        src:NOTADD5::varchar AS NOTADD5, 
                        src:POSTCODE::varchar AS POSTCODE, 
                        src:PROPADDRESS::varchar AS PROPADDRESS, 
                        src:SDLDATE::datetime AS SDLDATE, 
                        src:SERVICEREQUESTNUMBER::integer AS SERVICEREQUESTNUMBER, 
                        src:TOADDRESS::varchar AS TOADDRESS, 
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
    
                        
                src:DIRECTDEBITLETTERKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DIRECTDEBITLETTERKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER as strm))