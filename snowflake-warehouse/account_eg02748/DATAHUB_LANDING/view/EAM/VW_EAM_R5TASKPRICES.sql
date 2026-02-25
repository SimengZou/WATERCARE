CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5TASKPRICES AS SELECT
                        src:TPR_LASTSAVED::datetime AS TPR_LASTSAVED, 
                        src:TPR_ORG::varchar AS TPR_ORG, 
                        src:TPR_PRICE::numeric(38, 10) AS TPR_PRICE, 
                        src:TPR_REVISION::numeric(38, 10) AS TPR_REVISION, 
                        src:TPR_TASK::varchar AS TPR_TASK, 
                        src:TPR_UPDATECOUNT::numeric(38, 10) AS TPR_UPDATECOUNT, 
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
    
                        
                src:TPR_ORG,
                src:TPR_REVISION,
                src:TPR_TASK,
            src:TPR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TPR_ORG,
                src:TPR_REVISION,
                src:TPR_TASK  ORDER BY 
            src:TPR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5TASKPRICES as strm))