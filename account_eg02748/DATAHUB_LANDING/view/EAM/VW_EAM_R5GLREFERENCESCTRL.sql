CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5GLREFERENCESCTRL AS SELECT
                        src:GRC_DATATYPE::varchar AS GRC_DATATYPE, 
                        src:GRC_DISPLAYNAME::varchar AS GRC_DISPLAYNAME, 
                        src:GRC_DISPLAYSEQ::numeric(38, 10) AS GRC_DISPLAYSEQ, 
                        src:GRC_LASTSAVED::datetime AS GRC_LASTSAVED, 
                        src:GRC_LENGTH::numeric(38, 10) AS GRC_LENGTH, 
                        src:GRC_R5COLUMN::varchar AS GRC_R5COLUMN, 
                        src:GRC_UPDATECOUNT::numeric(38, 10) AS GRC_UPDATECOUNT, 
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
    
                        
                src:GRC_R5COLUMN,
            src:GRC_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:GRC_R5COLUMN  ORDER BY 
            src:GRC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5GLREFERENCESCTRL as strm))