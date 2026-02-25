CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PERMISSIONS AS SELECT
                        src:PRM_CREATED::datetime AS PRM_CREATED, 
                        src:PRM_DEFQUERY::varchar AS PRM_DEFQUERY, 
                        src:PRM_DELETE::varchar AS PRM_DELETE, 
                        src:PRM_FUNCTION::varchar AS PRM_FUNCTION, 
                        src:PRM_GROUP::varchar AS PRM_GROUP, 
                        src:PRM_INPUT::varchar AS PRM_INPUT, 
                        src:PRM_INSERT::varchar AS PRM_INSERT, 
                        src:PRM_LASTSAVED::datetime AS PRM_LASTSAVED, 
                        src:PRM_LOCAL::varchar AS PRM_LOCAL, 
                        src:PRM_OVERRULE::varchar AS PRM_OVERRULE, 
                        src:PRM_PRFILE::varchar AS PRM_PRFILE, 
                        src:PRM_PRINTER::varchar AS PRM_PRINTER, 
                        src:PRM_SCREEN::varchar AS PRM_SCREEN, 
                        src:PRM_SECURITYDDSPYID::numeric(38, 10) AS PRM_SECURITYDDSPYID, 
                        src:PRM_SELECT::varchar AS PRM_SELECT, 
                        src:PRM_UPDATE::varchar AS PRM_UPDATE, 
                        src:PRM_UPDATECOUNT::numeric(38, 10) AS PRM_UPDATECOUNT, 
                        src:PRM_UPDATED::datetime AS PRM_UPDATED, 
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
    
                        
                src:PRM_FUNCTION,
                src:PRM_GROUP,
            src:PRM_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PRM_FUNCTION,
                src:PRM_GROUP  ORDER BY 
            src:PRM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PERMISSIONS as strm))