CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5IPGROUPPERMISSIONS AS SELECT
                        src:IPG_ACTIVE::varchar AS IPG_ACTIVE, 
                        src:IPG_GROUP::varchar AS IPG_GROUP, 
                        src:IPG_LASTSAVED::datetime AS IPG_LASTSAVED, 
                        src:IPG_PERMISSION::numeric(38, 10) AS IPG_PERMISSION, 
                        src:IPG_UPDATECOUNT::numeric(38, 10) AS IPG_UPDATECOUNT, 
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
    
                        
                src:IPG_GROUP,
                src:IPG_PERMISSION,
            src:IPG_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:IPG_GROUP,
                src:IPG_PERMISSION  ORDER BY 
            src:IPG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5IPGROUPPERMISSIONS as strm))