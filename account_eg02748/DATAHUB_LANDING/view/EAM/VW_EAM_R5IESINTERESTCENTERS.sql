CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5IESINTERESTCENTERS AS SELECT
                        src:ENI_CATEGORY::varchar AS ENI_CATEGORY, 
                        src:ENI_CODE::varchar AS ENI_CODE, 
                        src:ENI_DESC::varchar AS ENI_DESC, 
                        src:ENI_LASTSAVED::datetime AS ENI_LASTSAVED, 
                        src:ENI_NOTUSED::varchar AS ENI_NOTUSED, 
                        src:ENI_UPDATECOUNT::numeric(38, 10) AS ENI_UPDATECOUNT, 
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
    
                        
                src:ENI_CODE,
            src:ENI_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ENI_CODE  ORDER BY 
            src:ENI_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5IESINTERESTCENTERS as strm))