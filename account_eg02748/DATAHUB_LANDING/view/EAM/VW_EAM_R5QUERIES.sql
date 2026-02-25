CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5QUERIES AS SELECT
                        src:QUE_ASSET::varchar AS QUE_ASSET, 
                        src:QUE_CHART::varchar AS QUE_CHART, 
                        src:QUE_CODE::varchar AS QUE_CODE, 
                        src:QUE_DESC::varchar AS QUE_DESC, 
                        src:QUE_EQUIPMENTRANKING::varchar AS QUE_EQUIPMENTRANKING, 
                        src:QUE_INBOX::varchar AS QUE_INBOX, 
                        src:QUE_KPI::varchar AS QUE_KPI, 
                        src:QUE_LASTSAVED::datetime AS QUE_LASTSAVED, 
                        src:QUE_LOOKUP::varchar AS QUE_LOOKUP, 
                        src:QUE_NORMAL::varchar AS QUE_NORMAL, 
                        src:QUE_NOTE::varchar AS QUE_NOTE, 
                        src:QUE_TEXT::varchar AS QUE_TEXT, 
                        src:QUE_UPDATECOUNT::numeric(38, 10) AS QUE_UPDATECOUNT, 
                        src:QUE_UPDATED::datetime AS QUE_UPDATED, 
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
    
                        
                src:QUE_CODE,
            src:QUE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:QUE_CODE  ORDER BY 
            src:QUE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5QUERIES as strm))