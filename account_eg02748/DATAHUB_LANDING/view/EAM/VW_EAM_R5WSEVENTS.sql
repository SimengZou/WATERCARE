CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WSEVENTS AS SELECT
                        src:WSE_BASE_EVENT::varchar AS WSE_BASE_EVENT, 
                        src:WSE_CODE::varchar AS WSE_CODE, 
                        src:WSE_DESC::varchar AS WSE_DESC, 
                        src:WSE_FILTER_PROCESSOR::varchar AS WSE_FILTER_PROCESSOR, 
                        src:WSE_LASTSAVED::datetime AS WSE_LASTSAVED, 
                        src:WSE_MEKEY::varchar AS WSE_MEKEY, 
                        src:WSE_MSG_TEMPLATE::varchar AS WSE_MSG_TEMPLATE, 
                        src:WSE_UPDATECOUNT::numeric(38, 10) AS WSE_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:WSE_CODE,
            src:WSE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WSE_CODE  ORDER BY 
            src:WSE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WSEVENTS as strm))