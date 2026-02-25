CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_READINGEXPORTROUTEDETAIL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:ISEXPORTED::varchar AS ISEXPORTED, 
                        src:ISIMPORTED::varchar AS ISIMPORTED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFMETERS::integer AS NUMBEROFMETERS, 
                        src:READINGEXPORTACTIVITYKEY::integer AS READINGEXPORTACTIVITYKEY, 
                        src:READINGEXPORTROUTEDTLKEY::integer AS READINGEXPORTROUTEDTLKEY, 
                        src:READINGEXPORTROUTEKEY::integer AS READINGEXPORTROUTEKEY, 
                        src:RERUNFLAG::varchar AS RERUNFLAG, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STATUS::integer AS STATUS, 
                        src:STOPDTTM::datetime AS STOPDTTM, 
                        src:UNHANDLEDEXCEPDESC::varchar AS UNHANDLEDEXCEPDESC, 
                        src:USEREXCEPTIONKEY::integer AS USEREXCEPTIONKEY, 
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
    
                        
                src:READINGEXPORTROUTEDTLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:READINGEXPORTROUTEDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_READINGEXPORTROUTEDETAIL as strm))