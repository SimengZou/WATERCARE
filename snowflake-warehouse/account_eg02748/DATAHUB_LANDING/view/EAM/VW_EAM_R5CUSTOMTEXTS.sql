CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CUSTOMTEXTS AS SELECT
                        src:CTT_DATE::datetime AS CTT_DATE, 
                        src:CTT_LANG::varchar AS CTT_LANG, 
                        src:CTT_LASTSAVED::datetime AS CTT_LASTSAVED, 
                        src:CTT_LENGTH::numeric(38, 10) AS CTT_LENGTH, 
                        src:CTT_ORIGTEXT::varchar AS CTT_ORIGTEXT, 
                        src:CTT_POOL::varchar AS CTT_POOL, 
                        src:CTT_TEXT::varchar AS CTT_TEXT, 
                        src:CTT_UPDATECOUNT::numeric(38, 10) AS CTT_UPDATECOUNT, 
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
    
                        
                src:CTT_LANG,
                src:CTT_LENGTH,
                src:CTT_POOL,
            src:CTT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CTT_LANG,
                src:CTT_LENGTH,
                src:CTT_POOL  ORDER BY 
            src:CTT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CUSTOMTEXTS as strm))