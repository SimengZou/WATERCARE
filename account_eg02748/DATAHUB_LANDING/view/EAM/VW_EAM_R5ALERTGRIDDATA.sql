CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTGRIDDATA AS SELECT
                        src:AGD_ALERT::varchar AS AGD_ALERT, 
                        src:AGD_DATA::varchar AS AGD_DATA, 
                        src:AGD_DATASPYID::numeric(38, 10) AS AGD_DATASPYID, 
                        src:AGD_DATE::datetime AS AGD_DATE, 
                        src:AGD_DESCRIPTION::varchar AS AGD_DESCRIPTION, 
                        src:AGD_GRIDID::numeric(38, 10) AS AGD_GRIDID, 
                        src:AGD_LASTSAVED::datetime AS AGD_LASTSAVED, 
                        src:AGD_PK::varchar AS AGD_PK, 
                        src:AGD_RECIPIENT::varchar AS AGD_RECIPIENT, 
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
    
                        
                src:AGD_PK,
            src:AGD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AGD_PK  ORDER BY 
            src:AGD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTGRIDDATA as strm))