CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MRCS AS SELECT
                        src:MRC_CAPACITY::numeric(38, 10) AS MRC_CAPACITY, 
                        src:MRC_CLASS::varchar AS MRC_CLASS, 
                        src:MRC_CLASS_ORG::varchar AS MRC_CLASS_ORG, 
                        src:MRC_CODE::varchar AS MRC_CODE, 
                        src:MRC_CREATED::datetime AS MRC_CREATED, 
                        src:MRC_DEPOT::varchar AS MRC_DEPOT, 
                        src:MRC_DEPOT_ORG::varchar AS MRC_DEPOT_ORG, 
                        src:MRC_DESC::varchar AS MRC_DESC, 
                        src:MRC_DFLTSCREENER::varchar AS MRC_DFLTSCREENER, 
                        src:MRC_LASTSAVED::datetime AS MRC_LASTSAVED, 
                        src:MRC_NOTUSED::varchar AS MRC_NOTUSED, 
                        src:MRC_ORG::varchar AS MRC_ORG, 
                        src:MRC_SCHEDGROUP::varchar AS MRC_SCHEDGROUP, 
                        src:MRC_SEGMENTVALUE::varchar AS MRC_SEGMENTVALUE, 
                        src:MRC_STORE::varchar AS MRC_STORE, 
                        src:MRC_UPDATECOUNT::numeric(38, 10) AS MRC_UPDATECOUNT, 
                        src:MRC_UPDATED::datetime AS MRC_UPDATED, 
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
    
                        
                src:MRC_CODE,
            src:MRC_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MRC_CODE  ORDER BY 
            src:MRC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MRCS as strm))