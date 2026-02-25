CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PRODUCTVERSIONHISTORY AS SELECT
                        src:PVH_BUILD::varchar AS PVH_BUILD, 
                        src:PVH_BUILDDATE::varchar AS PVH_BUILDDATE, 
                        src:PVH_CHANGED::datetime AS PVH_CHANGED, 
                        src:PVH_DESC::varchar AS PVH_DESC, 
                        src:PVH_LASTSAVED::datetime AS PVH_LASTSAVED, 
                        src:PVH_PATCH::varchar AS PVH_PATCH, 
                        src:PVH_PRODUCTCODE::varchar AS PVH_PRODUCTCODE, 
                        src:PVH_UPDATECOUNT::numeric(38, 10) AS PVH_UPDATECOUNT, 
                        src:PVH_VERSION::varchar AS PVH_VERSION, 
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
    
                        
                src:PVH_CHANGED,
                src:PVH_PRODUCTCODE,
            src:PVH_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PVH_CHANGED,
                src:PVH_PRODUCTCODE  ORDER BY 
            src:PVH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PRODUCTVERSIONHISTORY as strm))