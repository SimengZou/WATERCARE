CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PRODUCTVERSION AS SELECT
                        src:PVS_BUILD::varchar AS PVS_BUILD, 
                        src:PVS_LASTSAVED::datetime AS PVS_LASTSAVED, 
                        src:PVS_PATCH::varchar AS PVS_PATCH, 
                        src:PVS_PRODUCTCODE::varchar AS PVS_PRODUCTCODE, 
                        src:PVS_PRODUCTDESC::varchar AS PVS_PRODUCTDESC, 
                        src:PVS_UPDATECOUNT::numeric(38, 10) AS PVS_UPDATECOUNT, 
                        src:PVS_VERSION::varchar AS PVS_VERSION, 
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
    
                        
                src:PVS_PRODUCTCODE,
            src:PVS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PVS_PRODUCTCODE  ORDER BY 
            src:PVS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PRODUCTVERSION as strm))