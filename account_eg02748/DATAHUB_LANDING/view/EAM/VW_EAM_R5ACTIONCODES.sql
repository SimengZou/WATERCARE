CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ACTIONCODES AS SELECT
                        src:ACC_CLASS::varchar AS ACC_CLASS, 
                        src:ACC_CLASS_ORG::varchar AS ACC_CLASS_ORG, 
                        src:ACC_CODE::varchar AS ACC_CODE, 
                        src:ACC_CREATED::datetime AS ACC_CREATED, 
                        src:ACC_DESC::varchar AS ACC_DESC, 
                        src:ACC_ENABLEWORKORDERS::varchar AS ACC_ENABLEWORKORDERS, 
                        src:ACC_GEN::varchar AS ACC_GEN, 
                        src:ACC_GROUP::varchar AS ACC_GROUP, 
                        src:ACC_LASTSAVED::datetime AS ACC_LASTSAVED, 
                        src:ACC_NOTUSED::varchar AS ACC_NOTUSED, 
                        src:ACC_PARTFAILURE::varchar AS ACC_PARTFAILURE, 
                        src:ACC_UPDATECOUNT::numeric(38, 10) AS ACC_UPDATECOUNT, 
                        src:ACC_UPDATED::datetime AS ACC_UPDATED, 
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
    
                        
                src:ACC_CODE,
            src:ACC_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACC_CODE  ORDER BY 
            src:ACC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ACTIONCODES as strm))