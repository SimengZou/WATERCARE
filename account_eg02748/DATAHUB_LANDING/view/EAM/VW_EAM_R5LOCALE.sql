CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5LOCALE AS SELECT
                        src:LOC_CODE::varchar AS LOC_CODE, 
                        src:LOC_DATEFMT::varchar AS LOC_DATEFMT, 
                        src:LOC_DECSYM::varchar AS LOC_DECSYM, 
                        src:LOC_DESC::varchar AS LOC_DESC, 
                        src:LOC_FRACTDIGITS::numeric(38, 10) AS LOC_FRACTDIGITS, 
                        src:LOC_GRPDIGITS::numeric(38, 10) AS LOC_GRPDIGITS, 
                        src:LOC_GRPSYM::varchar AS LOC_GRPSYM, 
                        src:LOC_LASTSAVED::datetime AS LOC_LASTSAVED, 
                        src:LOC_MONDECSYM::varchar AS LOC_MONDECSYM, 
                        src:LOC_MONFRACT::numeric(38, 10) AS LOC_MONFRACT, 
                        src:LOC_MONTGRPDIGITS::numeric(38, 10) AS LOC_MONTGRPDIGITS, 
                        src:LOC_MONTGRPSEPARATOR::varchar AS LOC_MONTGRPSEPARATOR, 
                        src:LOC_NEGSYM::varchar AS LOC_NEGSYM, 
                        src:LOC_POSSYM::varchar AS LOC_POSSYM, 
                        src:LOC_STARTDAY::numeric(38, 10) AS LOC_STARTDAY, 
                        src:LOC_UPDATECOUNT::numeric(38, 10) AS LOC_UPDATECOUNT, 
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
    
                        
                src:LOC_CODE,
            src:LOC_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LOC_CODE  ORDER BY 
            src:LOC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5LOCALE as strm))