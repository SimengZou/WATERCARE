CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALLTEXTS AS SELECT
                        src:ATX_CODE::varchar AS ATX_CODE, 
                        src:ATX_LANG::varchar AS ATX_LANG, 
                        src:ATX_LASTMODIFIED::datetime AS ATX_LASTMODIFIED, 
                        src:ATX_LASTSAVED::datetime AS ATX_LASTSAVED, 
                        src:ATX_TEXT::varchar AS ATX_TEXT, 
                        src:ATX_TEXTTYPE::varchar AS ATX_TEXTTYPE, 
                        src:ATX_UPDATECOUNT::numeric(38, 10) AS ATX_UPDATECOUNT, 
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
    
                        
                src:ATX_CODE,
                src:ATX_LANG,
                src:ATX_TEXTTYPE,
            src:ATX_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ATX_CODE,
                src:ATX_LANG,
                src:ATX_TEXTTYPE  ORDER BY 
            src:ATX_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALLTEXTS as strm))