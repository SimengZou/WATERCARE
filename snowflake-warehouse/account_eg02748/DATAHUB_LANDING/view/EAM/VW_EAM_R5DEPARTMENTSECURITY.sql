CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5DEPARTMENTSECURITY AS SELECT
                        src:DSE_LASTSAVED::datetime AS DSE_LASTSAVED, 
                        src:DSE_MRC::varchar AS DSE_MRC, 
                        src:DSE_READONLY::varchar AS DSE_READONLY, 
                        src:DSE_UPDATECOUNT::numeric(38, 10) AS DSE_UPDATECOUNT, 
                        src:DSE_UPDATED::datetime AS DSE_UPDATED, 
                        src:DSE_USER::varchar AS DSE_USER, 
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
    
                        
                src:DSE_MRC,
                src:DSE_USER,
            src:DSE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DSE_MRC,
                src:DSE_USER  ORDER BY 
            src:DSE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5DEPARTMENTSECURITY as strm))