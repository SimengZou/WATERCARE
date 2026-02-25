CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FUTUREDUPLICATES AS SELECT
                        src:FDP_DATE::datetime AS FDP_DATE, 
                        src:FDP_LASTSAVED::datetime AS FDP_LASTSAVED, 
                        src:FDP_PPOPK::numeric(38, 10) AS FDP_PPOPK, 
                        src:FDP_SEQNO::numeric(38, 10) AS FDP_SEQNO, 
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
    
                        
                src:FDP_PPOPK,
                src:FDP_SEQNO,
            src:FDP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FDP_PPOPK,
                src:FDP_SEQNO  ORDER BY 
            src:FDP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FUTUREDUPLICATES as strm))