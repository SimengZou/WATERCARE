CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MOBILEDBSYNC AS SELECT
                        src:MDS_DBLASTUPDATEDDATE::datetime AS MDS_DBLASTUPDATEDDATE, 
                        src:MDS_DOWNLOAD::varchar AS MDS_DOWNLOAD, 
                        src:MDS_FILENAME::varchar AS MDS_FILENAME, 
                        src:MDS_GRIDS_PROCESSED::numeric(38, 10) AS MDS_GRIDS_PROCESSED, 
                        src:MDS_LASTSAVED::datetime AS MDS_LASTSAVED, 
                        src:MDS_ORG::varchar AS MDS_ORG, 
                        src:MDS_STATUS::varchar AS MDS_STATUS, 
                        src:MDS_STATUS_MSG::varchar AS MDS_STATUS_MSG, 
                        src:MDS_SYNCID::numeric(38, 10) AS MDS_SYNCID, 
                        src:MDS_SYNC_REQUEST::varchar AS MDS_SYNC_REQUEST, 
                        src:MDS_UPDATECOUNT::numeric(38, 10) AS MDS_UPDATECOUNT, 
                        src:MDS_USER::varchar AS MDS_USER, 
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
    
                        
                src:MDS_SYNCID,
            src:MDS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MDS_SYNCID  ORDER BY 
            src:MDS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MOBILEDBSYNC as strm))