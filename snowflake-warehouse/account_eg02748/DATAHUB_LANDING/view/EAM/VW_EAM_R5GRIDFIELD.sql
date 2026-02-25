CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5GRIDFIELD AS SELECT
                        src:GFD_AGGFUNC::varchar AS GFD_AGGFUNC, 
                        src:GFD_BOTFUNCTION::varchar AS GFD_BOTFUNCTION, 
                        src:GFD_BOTNUMBER::numeric(38, 10) AS GFD_BOTNUMBER, 
                        src:GFD_CONTROLTYPE::varchar AS GFD_CONTROLTYPE, 
                        src:GFD_DEPENDENT::numeric(38, 10) AS GFD_DEPENDENT, 
                        src:GFD_FIELDID::numeric(38, 10) AS GFD_FIELDID, 
                        src:GFD_FIELDTYPE::varchar AS GFD_FIELDTYPE, 
                        src:GFD_GRIDID::numeric(38, 10) AS GFD_GRIDID, 
                        src:GFD_HEADERLOCATION::varchar AS GFD_HEADERLOCATION, 
                        src:GFD_LASTSAVED::datetime AS GFD_LASTSAVED, 
                        src:GFD_OCCURRENCE::numeric(38, 10) AS GFD_OCCURRENCE, 
                        src:GFD_SCRIPTEVENT::varchar AS GFD_SCRIPTEVENT, 
                        src:GFD_SECENTITY::varchar AS GFD_SECENTITY, 
                        src:GFD_TAGNAME::varchar AS GFD_TAGNAME, 
                        src:GFD_TAGPARAMS::varchar AS GFD_TAGPARAMS, 
                        src:GFD_UPDATECOUNT::numeric(38, 10) AS GFD_UPDATECOUNT, 
                        src:GFD_UPDATED::datetime AS GFD_UPDATED, 
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
    
                        
                src:GFD_FIELDID,
                src:GFD_GRIDID,
                src:GFD_OCCURRENCE,
            src:GFD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:GFD_FIELDID,
                src:GFD_GRIDID,
                src:GFD_OCCURRENCE  ORDER BY 
            src:GFD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5GRIDFIELD as strm))