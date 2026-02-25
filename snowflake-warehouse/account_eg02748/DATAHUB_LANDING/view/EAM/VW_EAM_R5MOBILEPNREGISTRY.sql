CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MOBILEPNREGISTRY AS SELECT
                        src:MPN_APPID::varchar AS MPN_APPID, 
                        src:MPN_CREATED::datetime AS MPN_CREATED, 
                        src:MPN_CREATEDBY::varchar AS MPN_CREATEDBY, 
                        src:MPN_DEVICEID::varchar AS MPN_DEVICEID, 
                        src:MPN_LASTSAVED::datetime AS MPN_LASTSAVED, 
                        src:MPN_PLATFORM::varchar AS MPN_PLATFORM, 
                        src:MPN_TOKEN::varchar AS MPN_TOKEN, 
                        src:MPN_UPDATECOUNT::numeric(38, 10) AS MPN_UPDATECOUNT, 
                        src:MPN_UPDATED::datetime AS MPN_UPDATED, 
                        src:MPN_UPDATEDBY::varchar AS MPN_UPDATEDBY, 
                        src:MPN_USER::varchar AS MPN_USER, 
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
    
                        
                src:MPN_APPID,
                src:MPN_DEVICEID,
            src:MPN_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MPN_APPID,
                src:MPN_DEVICEID  ORDER BY 
            src:MPN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MOBILEPNREGISTRY as strm))