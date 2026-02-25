CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_BUILDING_BLDGPROCESSSTATE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGDEFNKEY::integer AS APBLDGDEFNKEY, 
                        src:APBLDGPROCESSSTATEKEY::integer AS APBLDGPROCESSSTATEKEY, 
                        src:APPLEXPDATECALCREQD::varchar AS APPLEXPDATECALCREQD, 
                        src:BLDGAPPLSTATUS::varchar AS BLDGAPPLSTATUS, 
                        src:CODE::varchar AS CODE, 
                        src:COMPLETEDSTATEFLAG::varchar AS COMPLETEDSTATEFLAG, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPLAYORDER::integer AS DISPLAYORDER, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INITIALSTATEFLAG::varchar AS INITIALSTATEFLAG, 
                        src:ISPUBLIC::varchar AS ISPUBLIC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTSTATEFRMLA::integer AS NEXTSTATEFRMLA, 
                        src:NEXTSTATEKEY::integer AS NEXTSTATEKEY, 
                        src:SETSTATDATE::varchar AS SETSTATDATE, 
                        src:STAT::varchar AS STAT, 
                        src:STATDATETYPE::integer AS STATDATETYPE, 
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
    
                        
                src:APBLDGPROCESSSTATEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APBLDGPROCESSSTATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_BUILDING_BLDGPROCESSSTATE as strm))