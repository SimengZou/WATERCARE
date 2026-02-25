CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5HYPERLINK AS SELECT
                        src:HYP_DATASPY::numeric(38, 10) AS HYP_DATASPY, 
                        src:HYP_DESTELEMENTID::varchar AS HYP_DESTELEMENTID, 
                        src:HYP_DESTPAGENAME::varchar AS HYP_DESTPAGENAME, 
                        src:HYP_LASTSAVED::datetime AS HYP_LASTSAVED, 
                        src:HYP_LINKNAME::varchar AS HYP_LINKNAME, 
                        src:HYP_PERFORMEXACTQUERY::varchar AS HYP_PERFORMEXACTQUERY, 
                        src:HYP_PK::numeric(38, 10) AS HYP_PK, 
                        src:HYP_SCREENMODE::varchar AS HYP_SCREENMODE, 
                        src:HYP_SOURCEELEMENTID::varchar AS HYP_SOURCEELEMENTID, 
                        src:HYP_SOURCEPAGENAME::varchar AS HYP_SOURCEPAGENAME, 
                        src:HYP_SRCLINENUMBER::numeric(38, 10) AS HYP_SRCLINENUMBER, 
                        src:HYP_UPDATECOUNT::numeric(38, 10) AS HYP_UPDATECOUNT, 
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
    
                        
                src:HYP_PK,
            src:HYP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HYP_PK  ORDER BY 
            src:HYP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5HYPERLINK as strm))