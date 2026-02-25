CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ROUTOBJECTS AS SELECT
                        src:ROB_FROMGEOREF::varchar AS ROB_FROMGEOREF, 
                        src:ROB_FROMPOINT::numeric(38, 10) AS ROB_FROMPOINT, 
                        src:ROB_FROMREFDESC::varchar AS ROB_FROMREFDESC, 
                        src:ROB_FROM_OFFSET::numeric(38, 10) AS ROB_FROM_OFFSET, 
                        src:ROB_FROM_OFFSET_DIRECTION::varchar AS ROB_FROM_OFFSET_DIRECTION, 
                        src:ROB_FROM_REFERENCE::numeric(38, 10) AS ROB_FROM_REFERENCE, 
                        src:ROB_LASTSAVED::datetime AS ROB_LASTSAVED, 
                        src:ROB_LINE::numeric(38, 10) AS ROB_LINE, 
                        src:ROB_OBJECT::varchar AS ROB_OBJECT, 
                        src:ROB_OBJECT_ORG::varchar AS ROB_OBJECT_ORG, 
                        src:ROB_OBRTYPE::varchar AS ROB_OBRTYPE, 
                        src:ROB_OBTYPE::varchar AS ROB_OBTYPE, 
                        src:ROB_REVISION::numeric(38, 10) AS ROB_REVISION, 
                        src:ROB_ROUTE::varchar AS ROB_ROUTE, 
                        src:ROB_TOGEOREF::varchar AS ROB_TOGEOREF, 
                        src:ROB_TOPOINT::numeric(38, 10) AS ROB_TOPOINT, 
                        src:ROB_TOREFDESC::varchar AS ROB_TOREFDESC, 
                        src:ROB_TO_OFFSET::numeric(38, 10) AS ROB_TO_OFFSET, 
                        src:ROB_TO_OFFSET_DIRECTION::varchar AS ROB_TO_OFFSET_DIRECTION, 
                        src:ROB_TO_REFERENCE::numeric(38, 10) AS ROB_TO_REFERENCE, 
                        src:ROB_UPDATECOUNT::numeric(38, 10) AS ROB_UPDATECOUNT, 
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
    
                        
                src:ROB_LINE,
                src:ROB_REVISION,
                src:ROB_ROUTE,
            src:ROB_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ROB_LINE,
                src:ROB_REVISION,
                src:ROB_ROUTE  ORDER BY 
            src:ROB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ROUTOBJECTS as strm))