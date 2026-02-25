CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MOBILEUSERCONFIGS AS SELECT
                        src:MUC_CODE::varchar AS MUC_CODE, 
                        src:MUC_COMPTYPE::varchar AS MUC_COMPTYPE, 
                        src:MUC_CREATED::datetime AS MUC_CREATED, 
                        src:MUC_DESK::varchar AS MUC_DESK, 
                        src:MUC_GROUP::varchar AS MUC_GROUP, 
                        src:MUC_LASTSAVED::datetime AS MUC_LASTSAVED, 
                        src:MUC_RCODE::varchar AS MUC_RCODE, 
                        src:MUC_UPDATECOUNT::numeric(38, 10) AS MUC_UPDATECOUNT, 
                        src:MUC_UPDATED::datetime AS MUC_UPDATED, 
                        src:MUC_USER::varchar AS MUC_USER, 
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
    
                        
                src:MUC_CODE,
                src:MUC_DESK,
                src:MUC_GROUP,
                src:MUC_USER,
            src:MUC_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MUC_CODE,
                src:MUC_DESK,
                src:MUC_GROUP,
                src:MUC_USER  ORDER BY 
            src:MUC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MOBILEUSERCONFIGS as strm))