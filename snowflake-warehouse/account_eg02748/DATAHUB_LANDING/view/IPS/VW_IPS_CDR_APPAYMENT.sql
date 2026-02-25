CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_APPAYMENT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMTCONF::numeric(38, 10) AS AMTCONF, 
                        src:APKEY::integer AS APKEY, 
                        src:APPAYKEY::integer AS APPAYKEY, 
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
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:CONFDTTM::datetime AS CONFDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESCROWACCTKEY::integer AS ESCROWACCTKEY, 
                        src:ESCROWNO::varchar AS ESCROWNO, 
                        src:ESCROWTRNNO::integer AS ESCROWTRNNO, 
                        src:MISCISSUED::varchar AS MISCISSUED, 
                        src:MISCREFNO::varchar AS MISCREFNO, 
                        src:MISCTYPE::varchar AS MISCTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYAMT::numeric(38, 10) AS PAYAMT, 
                        src:PAYDTTM::datetime AS PAYDTTM, 
                        src:PAYMENTDISTRIBUTION::integer AS PAYMENTDISTRIBUTION, 
                        src:PAYMENTMETHOD::integer AS PAYMENTMETHOD, 
                        src:PAYMETHOD::varchar AS PAYMETHOD, 
                        src:REGTRANNO::integer AS REGTRANNO, 
                        src:SRCBGTNOKEY::integer AS SRCBGTNOKEY, 
                        src:STAT::varchar AS STAT, 
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
    
                        
                src:APPAYKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPAYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_APPAYMENT as strm))