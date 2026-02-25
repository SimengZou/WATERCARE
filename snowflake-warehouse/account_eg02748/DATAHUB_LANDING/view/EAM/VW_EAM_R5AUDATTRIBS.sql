CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5AUDATTRIBS AS SELECT
                        src:AAT_CODE::numeric(38, 10) AS AAT_CODE, 
                        src:AAT_COLUMN::varchar AS AAT_COLUMN, 
                        src:AAT_COMMENT::varchar AS AAT_COMMENT, 
                        src:AAT_DELETE::varchar AS AAT_DELETE, 
                        src:AAT_ENTEREDBY::varchar AS AAT_ENTEREDBY, 
                        src:AAT_INSERT::varchar AS AAT_INSERT, 
                        src:AAT_LASTSAVED::datetime AS AAT_LASTSAVED, 
                        src:AAT_TABLE::varchar AS AAT_TABLE, 
                        src:AAT_TEXT::varchar AS AAT_TEXT, 
                        src:AAT_UPDATE::varchar AS AAT_UPDATE, 
                        src:AAT_UPDATECOUNT::numeric(38, 10) AS AAT_UPDATECOUNT, 
                        src:AAT_UPDATED::datetime AS AAT_UPDATED, 
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
    
                        
                src:AAT_CODE,
            src:AAT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AAT_CODE  ORDER BY 
            src:AAT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5AUDATTRIBS as strm))