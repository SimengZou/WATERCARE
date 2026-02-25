CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PMPSESSIONDATES AS SELECT
                        src:PPD_DUEDATE::datetime AS PPD_DUEDATE, 
                        src:PPD_ISCALDATE::varchar AS PPD_ISCALDATE, 
                        src:PPD_ISPROJECTED::varchar AS PPD_ISPROJECTED, 
                        src:PPD_LASTSAVED::datetime AS PPD_LASTSAVED, 
                        src:PPD_LINE::numeric(38, 10) AS PPD_LINE, 
                        src:PPD_PPSPK::numeric(38, 10) AS PPD_PPSPK, 
                        src:PPD_UPDATECOUNT::numeric(38, 10) AS PPD_UPDATECOUNT, 
                        src:PPD_WORKORDER::varchar AS PPD_WORKORDER, 
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
    
                        
                src:PPD_DUEDATE,
                src:PPD_LINE,
                src:PPD_PPSPK,
            src:PPD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PPD_DUEDATE,
                src:PPD_LINE,
                src:PPD_PPSPK  ORDER BY 
            src:PPD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PMPSESSIONDATES as strm))