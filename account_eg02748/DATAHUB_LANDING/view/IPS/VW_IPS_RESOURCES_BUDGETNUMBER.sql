CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_BUDGETNUMBER AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BUDGETNUMBERKEY::integer AS BUDGETNUMBERKEY, 
                        src:CODE::varchar AS CODE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPT::varchar AS DESCRIPT, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:SEGMENT1::varchar AS SEGMENT1, 
                        src:SEGMENT10::varchar AS SEGMENT10, 
                        src:SEGMENT11::varchar AS SEGMENT11, 
                        src:SEGMENT12::varchar AS SEGMENT12, 
                        src:SEGMENT13::varchar AS SEGMENT13, 
                        src:SEGMENT14::varchar AS SEGMENT14, 
                        src:SEGMENT15::varchar AS SEGMENT15, 
                        src:SEGMENT2::varchar AS SEGMENT2, 
                        src:SEGMENT3::varchar AS SEGMENT3, 
                        src:SEGMENT4::varchar AS SEGMENT4, 
                        src:SEGMENT5::varchar AS SEGMENT5, 
                        src:SEGMENT6::varchar AS SEGMENT6, 
                        src:SEGMENT7::varchar AS SEGMENT7, 
                        src:SEGMENT8::varchar AS SEGMENT8, 
                        src:SEGMENT9::varchar AS SEGMENT9, 
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
    
                        
                src:BUDGETNUMBERKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BUDGETNUMBERKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_BUDGETNUMBER as strm))