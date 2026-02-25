CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_USE_USEREVIEWTYPE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDCONDFRMLA::integer AS ADDCONDFRMLA, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APUSEDEFNKEY::integer AS APUSEDEFNKEY, 
                        src:APUSEPROCESSSTATEKEY::integer AS APUSEPROCESSSTATEKEY, 
                        src:APUSEREVIEWTYPEKEY::integer AS APUSEREVIEWTYPEKEY, 
                        src:ASSGNGRPFRMLA::integer AS ASSGNGRPFRMLA, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEPT::varchar AS DEPT, 
                        src:DESCRIPT::varchar AS DESCRIPT, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INTERNALONLYFLAG::varchar AS INTERNALONLYFLAG, 
                        src:ISPUBLIC::varchar AS ISPUBLIC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PLANCOPYNO::integer AS PLANCOPYNO, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:REVIEWTYPE::varchar AS REVIEWTYPE, 
                        src:SHOWINPORTAL::varchar AS SHOWINPORTAL, 
                        src:SUSPDAYS::integer AS SUSPDAYS, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKDAY::varchar AS WORKDAY, 
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
    
                        
                src:APUSEREVIEWTYPEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APUSEREVIEWTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_USE_USEREVIEWTYPE as strm))