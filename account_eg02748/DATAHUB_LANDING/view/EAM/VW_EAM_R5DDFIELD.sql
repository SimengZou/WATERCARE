CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5DDFIELD AS SELECT
                        src:DDF_DATATYPE::varchar AS DDF_DATATYPE, 
                        src:DDF_DESC::varchar AS DDF_DESC, 
                        src:DDF_FIELDID::numeric(38, 10) AS DDF_FIELDID, 
                        src:DDF_LASTSAVED::datetime AS DDF_LASTSAVED, 
                        src:DDF_LVGRID::varchar AS DDF_LVGRID, 
                        src:DDF_RENTITY::varchar AS DDF_RENTITY, 
                        src:DDF_SOURCENAME::varchar AS DDF_SOURCENAME, 
                        src:DDF_TABLENAME::varchar AS DDF_TABLENAME, 
                        src:DDF_UPDATECOUNT::numeric(38, 10) AS DDF_UPDATECOUNT, 
                        src:DDF_VALUEMAPID::numeric(38, 10) AS DDF_VALUEMAPID, 
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
    
                        
                src:DDF_FIELDID,
            src:DDF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DDF_FIELDID  ORDER BY 
            src:DDF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5DDFIELD as strm))