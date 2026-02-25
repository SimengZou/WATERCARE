CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_BUILDING_BLDGAPPLTYPE AS SELECT
                        src:ACCID::integer AS ACCID, 
                        src:ACCOUNTTYPECONFIGURATION::integer AS ACCOUNTTYPECONFIGURATION, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APAPPLTYPECATKEY::integer AS APAPPLTYPECATKEY, 
                        src:APBLDGDEFNKEY::integer AS APBLDGDEFNKEY, 
                        src:APDESC::varchar AS APDESC, 
                        src:APNOPREFIX::varchar AS APNOPREFIX, 
                        src:APTYPE::varchar AS APTYPE, 
                        src:AUTOFILL::varchar AS AUTOFILL, 
                        src:BILLINGACCOUNTTYPE::integer AS BILLINGACCOUNTTYPE, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:DPLEVEL::integer AS DPLEVEL, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:EXPDATEFRMLA::integer AS EXPDATEFRMLA, 
                        src:GISICON::varchar AS GISICON, 
                        src:HASSUPROVR::varchar AS HASSUPROVR, 
                        src:INTERNALONLYFLAG::varchar AS INTERNALONLYFLAG, 
                        src:ISPUBLIC::varchar AS ISPUBLIC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELHOMEFLAG::varchar AS MODELHOMEFLAG, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:PRIMARYSITETYPE::integer AS PRIMARYSITETYPE, 
                        src:RESPACCOUNTHOLDER::integer AS RESPACCOUNTHOLDER, 
                        src:SHOWINPORTAL::varchar AS SHOWINPORTAL, 
                        src:SUBMITINPORTAL::varchar AS SUBMITINPORTAL, 
                        src:USEEST::varchar AS USEEST, 
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
    
                        
                src:APBLDGDEFNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APBLDGDEFNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_BUILDING_BLDGAPPLTYPE as strm))