CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5LABELFORMATS AS SELECT
                        src:LBL_CLASS::varchar AS LBL_CLASS, 
                        src:LBL_CLASS_ORG::varchar AS LBL_CLASS_ORG, 
                        src:LBL_CODE::varchar AS LBL_CODE, 
                        src:LBL_DESC::varchar AS LBL_DESC, 
                        src:LBL_FIELDS::varchar AS LBL_FIELDS, 
                        src:LBL_FILENAME::varchar AS LBL_FILENAME, 
                        src:LBL_LASTSAVED::datetime AS LBL_LASTSAVED, 
                        src:LBL_PRINTERTYPE::varchar AS LBL_PRINTERTYPE, 
                        src:LBL_UPDATECOUNT::numeric(38, 10) AS LBL_UPDATECOUNT, 
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
    
                        
                src:LBL_CLASS,
                src:LBL_CLASS_ORG,
                src:LBL_CODE,
            src:LBL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LBL_CLASS,
                src:LBL_CLASS_ORG,
                src:LBL_CODE  ORDER BY 
            src:LBL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5LABELFORMATS as strm))