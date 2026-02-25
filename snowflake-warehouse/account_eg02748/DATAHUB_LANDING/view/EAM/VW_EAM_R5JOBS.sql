CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5JOBS AS SELECT
                        src:JOB_CLASS::varchar AS JOB_CLASS, 
                        src:JOB_DESCRIPTION::varchar AS JOB_DESCRIPTION, 
                        src:JOB_LASTSAVED::datetime AS JOB_LASTSAVED, 
                        src:JOB_NAME::varchar AS JOB_NAME, 
                        src:JOB_PARTNERID::varchar AS JOB_PARTNERID, 
                        src:JOB_TYPE::varchar AS JOB_TYPE, 
                        src:JOB_UPDATECOUNT::numeric(38, 10) AS JOB_UPDATECOUNT, 
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
    
                        
                src:JOB_NAME,
            src:JOB_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JOB_NAME  ORDER BY 
            src:JOB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5JOBS as strm))