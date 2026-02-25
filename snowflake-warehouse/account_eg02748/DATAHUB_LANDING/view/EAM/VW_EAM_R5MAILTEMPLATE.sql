CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MAILTEMPLATE AS SELECT
                        src:MAT_CODE::varchar AS MAT_CODE, 
                        src:MAT_DESC::varchar AS MAT_DESC, 
                        src:MAT_EMAIL::varchar AS MAT_EMAIL, 
                        src:MAT_FROMEMAIL::varchar AS MAT_FROMEMAIL, 
                        src:MAT_LASTSAVED::datetime AS MAT_LASTSAVED, 
                        src:MAT_MAIL::varchar AS MAT_MAIL, 
                        src:MAT_NOTEBOOKEMAILS::varchar AS MAT_NOTEBOOKEMAILS, 
                        src:MAT_PUSHNOTIFICATION::varchar AS MAT_PUSHNOTIFICATION, 
                        src:MAT_REPORT::varchar AS MAT_REPORT, 
                        src:MAT_SUBJECT::varchar AS MAT_SUBJECT, 
                        src:MAT_TEXT::varchar AS MAT_TEXT, 
                        src:MAT_UPDATECOUNT::numeric(38, 10) AS MAT_UPDATECOUNT, 
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
    
                        
                src:MAT_CODE,
            src:MAT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MAT_CODE  ORDER BY 
            src:MAT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MAILTEMPLATE as strm))