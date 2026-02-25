CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EDITIONFILTERS AS SELECT
                        src:EDF_CODE::varchar AS EDF_CODE, 
                        src:EDF_FILTER::varchar AS EDF_FILTER, 
                        src:EDF_LASTSAVED::datetime AS EDF_LASTSAVED, 
                        src:EDF_MEFLAG::varchar AS EDF_MEFLAG, 
                        src:EDF_TYPE::varchar AS EDF_TYPE, 
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
    
                        
                src:EDF_CODE,
                src:EDF_MEFLAG,
                src:EDF_TYPE,
            src:EDF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EDF_CODE,
                src:EDF_MEFLAG,
                src:EDF_TYPE  ORDER BY 
            src:EDF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EDITIONFILTERS as strm))