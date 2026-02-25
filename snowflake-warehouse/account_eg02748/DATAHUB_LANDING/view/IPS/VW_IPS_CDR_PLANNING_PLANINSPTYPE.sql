CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PLANNING_PLANINSPTYPE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDCONDFRMLA::integer AS ADDCONDFRMLA, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLANDEFNKEY::integer AS APPLANDEFNKEY, 
                        src:APPLANINSPTYPEKEY::integer AS APPLANINSPTYPEKEY, 
                        src:APPLANPROCESSSTATEKEY::integer AS APPLANPROCESSSTATEKEY, 
                        src:ASSGNGRPFRMLA::integer AS ASSGNGRPFRMLA, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DESCRIPT::varchar AS DESCRIPT, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INSPSCHEDCONDFRMLA::integer AS INSPSCHEDCONDFRMLA, 
                        src:INSPTYPE::varchar AS INSPTYPE, 
                        src:INTERNALONLYFLAG::varchar AS INTERNALONLYFLAG, 
                        src:ISPUBLIC::varchar AS ISPUBLIC, 
                        src:MAXINSPFEETYPEKEY::integer AS MAXINSPFEETYPEKEY, 
                        src:MAXINSPFRMLA::integer AS MAXINSPFRMLA, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORD::integer AS ORD, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:SHOWINPORTAL::varchar AS SHOWINPORTAL, 
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
    
                        
                src:APPLANINSPTYPEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPLANINSPTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PLANNING_PLANINSPTYPE as strm))