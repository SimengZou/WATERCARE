CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERVIEWS AS SELECT
                        src:UVW_ACTIVE::varchar AS UVW_ACTIVE, 
                        src:UVW_CODE::varchar AS UVW_CODE, 
                        src:UVW_DESC::varchar AS UVW_DESC, 
                        src:UVW_LASTSAVED::datetime AS UVW_LASTSAVED, 
                        src:UVW_NAME::varchar AS UVW_NAME, 
                        src:UVW_NOTE::varchar AS UVW_NOTE, 
                        src:UVW_STMT::varchar AS UVW_STMT, 
                        src:UVW_UPDATECOUNT::numeric(38, 10) AS UVW_UPDATECOUNT, 
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
    
                        
                src:UVW_CODE,
            src:UVW_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:UVW_CODE  ORDER BY 
            src:UVW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERVIEWS as strm))