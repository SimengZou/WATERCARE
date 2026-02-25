CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_DELINQUENCYMSALERT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALERTKEY::integer AS ALERTKEY, 
                        src:APPLYTOACCOUNT::varchar AS APPLYTOACCOUNT, 
                        src:APPLYTOCONTACT::varchar AS APPLYTOCONTACT, 
                        src:BELOWRESOLUTION::varchar AS BELOWRESOLUTION, 
                        src:CONDITIONFORMULA::integer AS CONDITIONFORMULA, 
                        src:DECREMENTRESOLUTION::varchar AS DECREMENTRESOLUTION, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISPLAYORDER::integer AS DISPLAYORDER, 
                        src:INCREMENTRESOLUTION::varchar AS INCREMENTRESOLUTION, 
                        src:MILESTONEALERTKEY::integer AS MILESTONEALERTKEY, 
                        src:MILESTONEKEY::integer AS MILESTONEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAIDOFFRESOLUTION::varchar AS PAIDOFFRESOLUTION, 
                        src:PERFORMON::varchar AS PERFORMON, 
                        src:REASON::varchar AS REASON, 
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
    
                        
                src:MILESTONEALERTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MILESTONEALERTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_DELINQUENCYMSALERT as strm))