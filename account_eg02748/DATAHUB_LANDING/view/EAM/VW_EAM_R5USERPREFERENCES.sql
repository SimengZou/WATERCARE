CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERPREFERENCES AS SELECT
                        src:USP_CODE::varchar AS USP_CODE, 
                        src:USP_LASTSAVED::datetime AS USP_LASTSAVED, 
                        src:USP_TYPE::varchar AS USP_TYPE, 
                        src:USP_UPDATECOUNT::numeric(38, 10) AS USP_UPDATECOUNT, 
                        src:USP_USER::varchar AS USP_USER, 
                        src:USP_VALUE::varchar AS USP_VALUE, 
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
    
                        
                src:USP_CODE,
                src:USP_TYPE,
                src:USP_USER,
            src:USP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:USP_CODE,
                src:USP_TYPE,
                src:USP_USER  ORDER BY 
            src:USP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERPREFERENCES as strm))