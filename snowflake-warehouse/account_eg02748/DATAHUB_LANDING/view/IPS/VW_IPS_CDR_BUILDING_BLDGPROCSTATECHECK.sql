CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_BUILDING_BLDGPROCSTATECHECK AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGPROCESSSTATEKEY::integer AS APBLDGPROCESSSTATEKEY, 
                        src:APBLDGPROCSTATECHKKEY::integer AS APBLDGPROCSTATECHKKEY, 
                        src:CHECKFRMLA::integer AS CHECKFRMLA, 
                        src:CHECKTITLE::varchar AS CHECKTITLE, 
                        src:CHECKTYPE::integer AS CHECKTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXCLUDEFROMPENDINGPORTAL::varchar AS EXCLUDEFROMPENDINGPORTAL, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INTERNALONLYFLAG::varchar AS INTERNALONLYFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
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
    
                        
                src:APBLDGPROCSTATECHKKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APBLDGPROCSTATECHKKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_BUILDING_BLDGPROCSTATECHECK as strm))