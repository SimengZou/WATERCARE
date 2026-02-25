CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USAGE AS SELECT
                        src:SUS_ACTION::varchar AS SUS_ACTION, 
                        src:SUS_ACTION_DATE::datetime AS SUS_ACTION_DATE, 
                        src:SUS_ID::varchar AS SUS_ID, 
                        src:SUS_LASTSAVED::datetime AS SUS_LASTSAVED, 
                        src:SUS_PROCESSING_TIME::numeric(38, 10) AS SUS_PROCESSING_TIME, 
                        src:SUS_SERVERID::varchar AS SUS_SERVERID, 
                        src:SUS_SESSIONID::numeric(38, 10) AS SUS_SESSIONID, 
                        src:SUS_SUBTYPE::varchar AS SUS_SUBTYPE, 
                        src:SUS_TARGET::varchar AS SUS_TARGET, 
                        src:SUS_TARGET_PARENT::varchar AS SUS_TARGET_PARENT, 
                        src:SUS_TARGET_TAB::varchar AS SUS_TARGET_TAB, 
                        src:SUS_TENANT::varchar AS SUS_TENANT, 
                        src:SUS_TYPE::varchar AS SUS_TYPE, 
                        src:SUS_UPDATECOUNT::numeric(38, 10) AS SUS_UPDATECOUNT, 
                        src:SUS_USERAGENT::varchar AS SUS_USERAGENT, 
                        src:SUS_USERID::varchar AS SUS_USERID, 
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
    
                        
                src:SUS_ID,
            src:SUS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SUS_ID  ORDER BY 
            src:SUS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USAGE as strm))