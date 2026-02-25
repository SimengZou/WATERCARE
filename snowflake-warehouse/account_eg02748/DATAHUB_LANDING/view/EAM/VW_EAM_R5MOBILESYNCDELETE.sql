CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MOBILESYNCDELETE AS SELECT
                        src:MSD_DELETED::datetime AS MSD_DELETED, 
                        src:MSD_KEYS::varchar AS MSD_KEYS, 
                        src:MSD_LASTSAVED::datetime AS MSD_LASTSAVED, 
                        src:MSD_ORG::varchar AS MSD_ORG, 
                        src:MSD_PK::numeric(38, 10) AS MSD_PK, 
                        src:MSD_RENTITY::varchar AS MSD_RENTITY, 
                        src:MSD_TABLENAME::varchar AS MSD_TABLENAME, 
                        src:MSD_UPDATECOUNT::numeric(38, 10) AS MSD_UPDATECOUNT, 
                        src:MSD_VALUES::varchar AS MSD_VALUES, 
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
    
                        
                src:MSD_PK,
            src:MSD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MSD_PK  ORDER BY 
            src:MSD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MOBILESYNCDELETE as strm))