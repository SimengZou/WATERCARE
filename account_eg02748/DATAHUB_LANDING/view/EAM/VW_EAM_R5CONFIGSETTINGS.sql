CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CONFIGSETTINGS AS SELECT
                        src:CFS_CODE::varchar AS CFS_CODE, 
                        src:CFS_COMPTYPE::varchar AS CFS_COMPTYPE, 
                        src:CFS_CONFIG::numeric(38, 10) AS CFS_CONFIG, 
                        src:CFS_CREATED::datetime AS CFS_CREATED, 
                        src:CFS_DESK::varchar AS CFS_DESK, 
                        src:CFS_GROUP::varchar AS CFS_GROUP, 
                        src:CFS_LASTSAVED::datetime AS CFS_LASTSAVED, 
                        src:CFS_UPDATECOUNT::numeric(38, 10) AS CFS_UPDATECOUNT, 
                        src:CFS_UPDATED::datetime AS CFS_UPDATED, 
                        src:CFS_USER::varchar AS CFS_USER, 
                        src:CFS_XMLCONFIG::varchar AS CFS_XMLCONFIG, 
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
    
                        
                src:CFS_CODE,
                src:CFS_COMPTYPE,
                src:CFS_CONFIG,
                src:CFS_DESK,
                src:CFS_GROUP,
                src:CFS_USER,
            src:CFS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CFS_CODE,
                src:CFS_COMPTYPE,
                src:CFS_CONFIG,
                src:CFS_DESK,
                src:CFS_GROUP,
                src:CFS_USER  ORDER BY 
            src:CFS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CONFIGSETTINGS as strm))