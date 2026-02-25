CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EXTMENULANG AS SELECT
                        src:EML_EXTMENU::varchar AS EML_EXTMENU, 
                        src:EML_LANG::varchar AS EML_LANG, 
                        src:EML_LASTSAVED::datetime AS EML_LASTSAVED, 
                        src:EML_TEXT::varchar AS EML_TEXT, 
                        src:EML_TRANS::varchar AS EML_TRANS, 
                        src:EML_UPDATECOUNT::numeric(38, 10) AS EML_UPDATECOUNT, 
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
    
                        
                src:EML_EXTMENU,
                src:EML_LANG,
            src:EML_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EML_EXTMENU,
                src:EML_LANG  ORDER BY 
            src:EML_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EXTMENULANG as strm))