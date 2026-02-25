CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ACDPARAMS AS SELECT
                        src:ADP_DATATYPE::varchar AS ADP_DATATYPE, 
                        src:ADP_DEFAULT::varchar AS ADP_DEFAULT, 
                        src:ADP_DESCSQL::varchar AS ADP_DESCSQL, 
                        src:ADP_HINT::varchar AS ADP_HINT, 
                        src:ADP_LASTSAVED::datetime AS ADP_LASTSAVED, 
                        src:ADP_LENGTH::numeric(38, 10) AS ADP_LENGTH, 
                        src:ADP_LOVSQL::varchar AS ADP_LOVSQL, 
                        src:ADP_PROMPT::varchar AS ADP_PROMPT, 
                        src:ADP_PROMPTX::numeric(38, 10) AS ADP_PROMPTX, 
                        src:ADP_PROMPTY::numeric(38, 10) AS ADP_PROMPTY, 
                        src:ADP_QUERY::varchar AS ADP_QUERY, 
                        src:ADP_REQUIRED::varchar AS ADP_REQUIRED, 
                        src:ADP_SEGMENT::varchar AS ADP_SEGMENT, 
                        src:ADP_SEGMENTX::numeric(38, 10) AS ADP_SEGMENTX, 
                        src:ADP_SEGMENTY::numeric(38, 10) AS ADP_SEGMENTY, 
                        src:ADP_SEQ::numeric(38, 10) AS ADP_SEQ, 
                        src:ADP_UPDATECOUNT::numeric(38, 10) AS ADP_UPDATECOUNT, 
                        src:ADP_VALUELOOKUP::varchar AS ADP_VALUELOOKUP, 
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
    
                        
                src:ADP_SEGMENT,
            src:ADP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ADP_SEGMENT  ORDER BY 
            src:ADP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ACDPARAMS as strm))