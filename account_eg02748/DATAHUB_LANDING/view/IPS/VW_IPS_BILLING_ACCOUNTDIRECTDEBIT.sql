CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCOUNTDIRECTDEBIT AS SELECT
                        src:ACCOUNTDIRECTDEBITKEY::integer AS ACCOUNTDIRECTDEBITKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CONFIGUREDVALUE::integer AS CONFIGUREDVALUE, 
                        src:DEBITAMOUNT::numeric(38, 10) AS DEBITAMOUNT, 
                        src:DEBITFREQUENCY::integer AS DEBITFREQUENCY, 
                        src:DEBITREMANINGAMOUNT::varchar AS DEBITREMANINGAMOUNT, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIRECDEBITCYCLE::integer AS DIRECDEBITCYCLE, 
                        src:DIRECTDEBITSCHEDULEPLAN::integer AS DIRECTDEBITSCHEDULEPLAN, 
                        src:DONATEFLAG::varchar AS DONATEFLAG, 
                        src:DONATIONAMOUNT::numeric(38, 10) AS DONATIONAMOUNT, 
                        src:EFFECTIVEDATE::datetime AS EFFECTIVEDATE, 
                        src:EXPIREDATE::datetime AS EXPIREDATE, 
                        src:EXPIREREASON::varchar AS EXPIREREASON, 
                        src:EXTRACTDATE::datetime AS EXTRACTDATE, 
                        src:IDENTITYBANKINGKEY::integer AS IDENTITYBANKINGKEY, 
                        src:LASTEXTRACTDATE::datetime AS LASTEXTRACTDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTEXTRACTDATE::datetime AS NEXTEXTRACTDATE, 
                        src:REFRESHBILLAMOUNT::varchar AS REFRESHBILLAMOUNT, 
                        src:TRANSMITDATE::datetime AS TRANSMITDATE, 
                        src:TRANSMITTINGHOLDFLAG::varchar AS TRANSMITTINGHOLDFLAG, 
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
    
                        
                src:ACCOUNTDIRECTDEBITKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTDIRECTDEBITKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCOUNTDIRECTDEBIT as strm))