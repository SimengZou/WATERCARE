CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ERRSOURCE AS SELECT
                        src:ERS_CODE::varchar AS ERS_CODE, 
                        src:ERS_DESC::varchar AS ERS_DESC, 
                        src:ERS_LASTSAVED::datetime AS ERS_LASTSAVED, 
                        src:ERS_NAME::varchar AS ERS_NAME, 
                        src:ERS_NUMBER::numeric(38, 10) AS ERS_NUMBER, 
                        src:ERS_SOURCE::varchar AS ERS_SOURCE, 
                        src:ERS_TYPE::varchar AS ERS_TYPE, 
                        src:ERS_UPDATECOUNT::numeric(38, 10) AS ERS_UPDATECOUNT, 
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
    
                        
                src:ERS_NUMBER,
                src:ERS_SOURCE,
                src:ERS_TYPE,
            src:ERS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ERS_NUMBER,
                src:ERS_SOURCE,
                src:ERS_TYPE  ORDER BY 
            src:ERS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ERRSOURCE as strm))