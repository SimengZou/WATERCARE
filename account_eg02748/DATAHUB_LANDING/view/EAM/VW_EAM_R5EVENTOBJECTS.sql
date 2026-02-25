CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EVENTOBJECTS AS SELECT
                        src:EOB_DOWNTIME::varchar AS EOB_DOWNTIME, 
                        src:EOB_EVENT::varchar AS EOB_EVENT, 
                        src:EOB_FROMPOINT::numeric(38, 10) AS EOB_FROMPOINT, 
                        src:EOB_LASTSAVED::datetime AS EOB_LASTSAVED, 
                        src:EOB_LEVEL::numeric(38, 10) AS EOB_LEVEL, 
                        src:EOB_OBJECT::varchar AS EOB_OBJECT, 
                        src:EOB_OBJECT_ORG::varchar AS EOB_OBJECT_ORG, 
                        src:EOB_OBRTYPE::varchar AS EOB_OBRTYPE, 
                        src:EOB_OBTYPE::varchar AS EOB_OBTYPE, 
                        src:EOB_ROLLUP::varchar AS EOB_ROLLUP, 
                        src:EOB_TOPOINT::numeric(38, 10) AS EOB_TOPOINT, 
                        src:EOB_UPDATECOUNT::numeric(38, 10) AS EOB_UPDATECOUNT, 
                        src:EOB_WEIGHTPERCENTAGE::numeric(38, 10) AS EOB_WEIGHTPERCENTAGE, 
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
    
                        
                src:EOB_EVENT,
                src:EOB_OBJECT,
                src:EOB_OBJECT_ORG,
            src:EOB_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EOB_EVENT,
                src:EOB_OBJECT,
                src:EOB_OBJECT_ORG  ORDER BY 
            src:EOB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EVENTOBJECTS as strm))