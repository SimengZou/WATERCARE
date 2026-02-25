CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5REPORTSUBTOTAL AS SELECT
                        src:RST_BOTNUMBER::numeric(38, 10) AS RST_BOTNUMBER, 
                        src:RST_DATATYPE::varchar AS RST_DATATYPE, 
                        src:RST_FIELD::varchar AS RST_FIELD, 
                        src:RST_FUNCTION::varchar AS RST_FUNCTION, 
                        src:RST_GROUPLINE::numeric(38, 10) AS RST_GROUPLINE, 
                        src:RST_LASTSAVED::datetime AS RST_LASTSAVED, 
                        src:RST_LINE::numeric(38, 10) AS RST_LINE, 
                        src:RST_UPDATECOUNT::numeric(38, 10) AS RST_UPDATECOUNT, 
                        src:RST_UPDATED::datetime AS RST_UPDATED, 
                        src:RST_WIDTH::numeric(38, 10) AS RST_WIDTH, 
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
    
                        
                src:RST_FUNCTION,
                src:RST_GROUPLINE,
                src:RST_LINE,
            src:RST_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RST_FUNCTION,
                src:RST_GROUPLINE,
                src:RST_LINE  ORDER BY 
            src:RST_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5REPORTSUBTOTAL as strm))