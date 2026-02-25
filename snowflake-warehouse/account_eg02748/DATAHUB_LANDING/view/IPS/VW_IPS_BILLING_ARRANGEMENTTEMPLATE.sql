CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ARRANGEMENTTEMPLATE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ARRANGEDPAYMENTAMOUNT::numeric(38, 10) AS ARRANGEDPAYMENTAMOUNT, 
                        src:ARRANGEMENTCATEGORY::varchar AS ARRANGEMENTCATEGORY, 
                        src:ARRANGEMENTTEMPLATEKEY::integer AS ARRANGEMENTTEMPLATEKEY, 
                        src:ARRANGEMENTTYPE::varchar AS ARRANGEMENTTYPE, 
                        src:AVERAGEDPAYMENTFLAG::varchar AS AVERAGEDPAYMENTFLAG, 
                        src:DEFAULTTEMPLATEFLAG::varchar AS DEFAULTTEMPLATEFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:DOWNPAYMENTAMT::numeric(38, 10) AS DOWNPAYMENTAMT, 
                        src:DOWNPAYMENTDUEDAYS::integer AS DOWNPAYMENTDUEDAYS, 
                        src:FREQUENCY::varchar AS FREQUENCY, 
                        src:ID::varchar AS ID, 
                        src:INCLUDECURRENTFLAG::varchar AS INCLUDECURRENTFLAG, 
                        src:INCLUDEDEPOSITFLAG::varchar AS INCLUDEDEPOSITFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFPAYMENTS::integer AS NUMBEROFPAYMENTS, 
                        src:PAYOLDESTFIRSTFLAG::varchar AS PAYOLDESTFIRSTFLAG, 
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
    
                        
                src:ARRANGEMENTTEMPLATEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ARRANGEMENTTEMPLATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ARRANGEMENTTEMPLATE as strm))