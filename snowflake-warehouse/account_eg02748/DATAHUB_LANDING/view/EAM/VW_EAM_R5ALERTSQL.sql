CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTSQL AS SELECT
                        src:ALS_ABORTONFAILURE::varchar AS ALS_ABORTONFAILURE, 
                        src:ALS_ACTIVE::varchar AS ALS_ACTIVE, 
                        src:ALS_ALERT::varchar AS ALS_ALERT, 
                        src:ALS_COMMENT::varchar AS ALS_COMMENT, 
                        src:ALS_INCLUDEPREVIEW::varchar AS ALS_INCLUDEPREVIEW, 
                        src:ALS_LASTSAVED::datetime AS ALS_LASTSAVED, 
                        src:ALS_RTYPE::varchar AS ALS_RTYPE, 
                        src:ALS_STMT::varchar AS ALS_STMT, 
                        src:ALS_UPDATECOUNT::numeric(38, 10) AS ALS_UPDATECOUNT, 
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
    
                        
                src:ALS_ALERT,
                src:ALS_RTYPE,
            src:ALS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ALS_ALERT,
                src:ALS_RTYPE  ORDER BY 
            src:ALS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTSQL as strm))