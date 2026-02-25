CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5REPORTGROUPBY AS SELECT
                        src:RGP_DEFAULT::varchar AS RGP_DEFAULT, 
                        src:RGP_FUNCTION::varchar AS RGP_FUNCTION, 
                        src:RGP_GROUPFIELDS::varchar AS RGP_GROUPFIELDS, 
                        src:RGP_LASTSAVED::datetime AS RGP_LASTSAVED, 
                        src:RGP_LINE::numeric(38, 10) AS RGP_LINE, 
                        src:RGP_UPDATECOUNT::numeric(38, 10) AS RGP_UPDATECOUNT, 
                        src:RGP_UPDATED::datetime AS RGP_UPDATED, 
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
    
                        
                src:RGP_FUNCTION,
                src:RGP_LINE,
            src:RGP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RGP_FUNCTION,
                src:RGP_LINE  ORDER BY 
            src:RGP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5REPORTGROUPBY as strm))