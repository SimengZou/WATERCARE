CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_READINGIMPORTMETERDETAIL AS SELECT
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXCEPTIONPROCESSEDFLAG::varchar AS EXCEPTIONPROCESSEDFLAG, 
                        src:EXCEPTIONTYPE::integer AS EXCEPTIONTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORIGINALMETER::varchar AS ORIGINALMETER, 
                        src:POSITION::integer AS POSITION, 
                        src:PROBCODE::varchar AS PROBCODE, 
                        src:READERCODE::varchar AS READERCODE, 
                        src:READINGIMPORTACTIVITYKEY::integer AS READINGIMPORTACTIVITYKEY, 
                        src:READINGIMPORTMETERDTLKEY::integer AS READINGIMPORTMETERDTLKEY, 
                        src:READINGIMPORTROUTEDTLKEY::integer AS READINGIMPORTROUTEDTLKEY, 
                        src:READINGKEY::integer AS READINGKEY, 
                        src:READYTOBILLPROCESSEDFLAG::varchar AS READYTOBILLPROCESSEDFLAG, 
                        src:REGREADING::numeric(38, 10) AS REGREADING, 
                        src:REGTYPE::integer AS REGTYPE, 
                        src:SEQUENCENUMBER::integer AS SEQUENCENUMBER, 
                        src:UNHANDLEDEXCEPDESC::varchar AS UNHANDLEDEXCEPDESC, 
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
    
                        
                src:READINGIMPORTMETERDTLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:READINGIMPORTMETERDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_READINGIMPORTMETERDETAIL as strm))