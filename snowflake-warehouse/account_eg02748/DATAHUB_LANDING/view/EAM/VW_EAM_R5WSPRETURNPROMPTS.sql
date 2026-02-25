CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WSPRETURNPROMPTS AS SELECT
                        src:WPR_LASTSAVED::datetime AS WPR_LASTSAVED, 
                        src:WPR_MOBILEQUERYCODE::varchar AS WPR_MOBILEQUERYCODE, 
                        src:WPR_QUERYCODE::varchar AS WPR_QUERYCODE, 
                        src:WPR_SOURCESEQ::numeric(38, 10) AS WPR_SOURCESEQ, 
                        src:WPR_TARGETSEQ::numeric(38, 10) AS WPR_TARGETSEQ, 
                        src:WPR_UPDATECOUNT::numeric(38, 10) AS WPR_UPDATECOUNT, 
                        src:WPR_UPDATED::datetime AS WPR_UPDATED, 
                        src:WPR_WSPCODE::varchar AS WPR_WSPCODE, 
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
    
                        
                src:WPR_SOURCESEQ,
                src:WPR_TARGETSEQ,
                src:WPR_WSPCODE,
            src:WPR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WPR_SOURCESEQ,
                src:WPR_TARGETSEQ,
                src:WPR_WSPCODE  ORDER BY 
            src:WPR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WSPRETURNPROMPTS as strm))