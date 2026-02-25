CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5AUDITPK AS SELECT
                        src:APK_COLUMN::varchar AS APK_COLUMN, 
                        src:APK_DATATYPE::varchar AS APK_DATATYPE, 
                        src:APK_LASTSAVED::datetime AS APK_LASTSAVED, 
                        src:APK_SEQNO::numeric(38, 10) AS APK_SEQNO, 
                        src:APK_TABLE::varchar AS APK_TABLE, 
                        src:APK_UPDATECOUNT::numeric(38, 10) AS APK_UPDATECOUNT, 
                        src:APK_UPDATED::datetime AS APK_UPDATED, 
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
    
                        
                src:APK_SEQNO,
                src:APK_TABLE,
            src:APK_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APK_SEQNO,
                src:APK_TABLE  ORDER BY 
            src:APK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5AUDITPK as strm))