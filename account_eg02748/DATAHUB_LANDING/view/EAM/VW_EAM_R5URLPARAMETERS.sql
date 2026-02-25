CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5URLPARAMETERS AS SELECT
                        src:URL_ACTIVE::varchar AS URL_ACTIVE, 
                        src:URL_ALTERNATEPARAMETERNAME::varchar AS URL_ALTERNATEPARAMETERNAME, 
                        src:URL_LASTSAVED::datetime AS URL_LASTSAVED, 
                        src:URL_PARAMETERNAME::varchar AS URL_PARAMETERNAME, 
                        src:URL_PARAMETERVALUE::varchar AS URL_PARAMETERVALUE, 
                        src:URL_SYSTEM::varchar AS URL_SYSTEM, 
                        src:URL_TAB::varchar AS URL_TAB, 
                        src:URL_UPDATECOUNT::numeric(38, 10) AS URL_UPDATECOUNT, 
                        src:URL_USEFIELDVALUE::varchar AS URL_USEFIELDVALUE, 
                        src:URL_USERFUNCTION::varchar AS URL_USERFUNCTION, 
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
    
                        
                src:URL_PARAMETERNAME,
                src:URL_TAB,
                src:URL_USERFUNCTION,
            src:URL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:URL_PARAMETERNAME,
                src:URL_TAB,
                src:URL_USERFUNCTION  ORDER BY 
            src:URL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5URLPARAMETERS as strm))