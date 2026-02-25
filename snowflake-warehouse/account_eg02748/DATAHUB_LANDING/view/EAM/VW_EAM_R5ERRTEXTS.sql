CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ERRTEXTS AS SELECT
                        src:ERT_CODE::varchar AS ERT_CODE, 
                        src:ERT_LANG::varchar AS ERT_LANG, 
                        src:ERT_LASTSAVED::datetime AS ERT_LASTSAVED, 
                        src:ERT_TEXT::varchar AS ERT_TEXT, 
                        src:ERT_TRANSLATE::varchar AS ERT_TRANSLATE, 
                        src:ERT_UPDATECOUNT::numeric(38, 10) AS ERT_UPDATECOUNT, 
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
    
                        
                src:ERT_CODE,
                src:ERT_LANG,
            src:ERT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ERT_CODE,
                src:ERT_LANG  ORDER BY 
            src:ERT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ERRTEXTS as strm))