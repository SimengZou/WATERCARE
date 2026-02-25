CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_U5JOBPRRESPTIME AS SELECT
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:JBP_BUSINESSDAYS::varchar AS JBP_BUSINESSDAYS, 
                        src:JBP_CONTRACTOR::varchar AS JBP_CONTRACTOR, 
                        src:JBP_ORGANIZATION::varchar AS JBP_ORGANIZATION, 
                        src:JBP_WOCOMPLETIONTIME::numeric(38, 10) AS JBP_WOCOMPLETIONTIME, 
                        src:JBP_WOPRIORITY::varchar AS JBP_WOPRIORITY, 
                        src:JBP_WOPRIORITY_DESC::varchar AS JBP_WOPRIORITY_DESC, 
                        src:JBP_WORESOLUTIONTIME::numeric(38, 10) AS JBP_WORESOLUTIONTIME, 
                        src:JBP_WORESPONSETIME::numeric(38, 10) AS JBP_WORESPONSETIME, 
                        src:JBP_WOTYPE::varchar AS JBP_WOTYPE, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
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
    
                        
                src:JBP_CONTRACTOR,
                src:JBP_ORGANIZATION,
                src:JBP_WOPRIORITY,
                src:JBP_WOTYPE,
            src:LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JBP_CONTRACTOR,
                src:JBP_ORGANIZATION,
                src:JBP_WOPRIORITY,
                src:JBP_WOTYPE  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_U5JOBPRRESPTIME as strm))