CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WORKPACKAGES AS SELECT
                        src:WPK_CHANGED::varchar AS WPK_CHANGED, 
                        src:WPK_CLASS::varchar AS WPK_CLASS, 
                        src:WPK_CLASS_ORG::varchar AS WPK_CLASS_ORG, 
                        src:WPK_CODE::varchar AS WPK_CODE, 
                        src:WPK_DESC::varchar AS WPK_DESC, 
                        src:WPK_DUEDATE::datetime AS WPK_DUEDATE, 
                        src:WPK_DURATION::numeric(38, 10) AS WPK_DURATION, 
                        src:WPK_ESTWORKLOAD::numeric(38, 10) AS WPK_ESTWORKLOAD, 
                        src:WPK_FREQ::numeric(38, 10) AS WPK_FREQ, 
                        src:WPK_JOBCLASS::varchar AS WPK_JOBCLASS, 
                        src:WPK_JOBCLASS_ORG::varchar AS WPK_JOBCLASS_ORG, 
                        src:WPK_JOBTYPE::varchar AS WPK_JOBTYPE, 
                        src:WPK_LASTEVENT::varchar AS WPK_LASTEVENT, 
                        src:WPK_LASTSAVED::datetime AS WPK_LASTSAVED, 
                        src:WPK_MRC::varchar AS WPK_MRC, 
                        src:WPK_OBJECT::varchar AS WPK_OBJECT, 
                        src:WPK_OBJECT_ORG::varchar AS WPK_OBJECT_ORG, 
                        src:WPK_ORG::varchar AS WPK_ORG, 
                        src:WPK_PERFORMONDAY::numeric(38, 10) AS WPK_PERFORMONDAY, 
                        src:WPK_PERFORMONWEEK::varchar AS WPK_PERFORMONWEEK, 
                        src:WPK_PERIODUOM::varchar AS WPK_PERIODUOM, 
                        src:WPK_PERSONS::numeric(38, 10) AS WPK_PERSONS, 
                        src:WPK_PPMTYPE::varchar AS WPK_PPMTYPE, 
                        src:WPK_STATUS::varchar AS WPK_STATUS, 
                        src:WPK_TRADE::varchar AS WPK_TRADE, 
                        src:WPK_UPDATECOUNT::numeric(38, 10) AS WPK_UPDATECOUNT, 
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
    
                        
                src:WPK_CODE,
            src:WPK_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WPK_CODE  ORDER BY 
            src:WPK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WORKPACKAGES as strm))