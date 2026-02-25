CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PMPLANSESSION AS SELECT
                        src:PPS_DUEDATE::datetime AS PPS_DUEDATE, 
                        src:PPS_DUEDAYOFWEEK::numeric(38, 10) AS PPS_DUEDAYOFWEEK, 
                        src:PPS_DUEWEEKOFMONTH::varchar AS PPS_DUEWEEKOFMONTH, 
                        src:PPS_IGNOREFREQWARNING::varchar AS PPS_IGNOREFREQWARNING, 
                        src:PPS_IGNORERANGEWARNING::varchar AS PPS_IGNORERANGEWARNING, 
                        src:PPS_LASTSAVED::datetime AS PPS_LASTSAVED, 
                        src:PPS_LOCKED::varchar AS PPS_LOCKED, 
                        src:PPS_NESTINGREF::varchar AS PPS_NESTINGREF, 
                        src:PPS_PK::numeric(38, 10) AS PPS_PK, 
                        src:PPS_PMREVISION::numeric(38, 10) AS PPS_PMREVISION, 
                        src:PPS_PPOPK::numeric(38, 10) AS PPS_PPOPK, 
                        src:PPS_SESSIONID::numeric(38, 10) AS PPS_SESSIONID, 
                        src:PPS_UPDATECOUNT::numeric(38, 10) AS PPS_UPDATECOUNT, 
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
    
                        
                src:PPS_PK,
            src:PPS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PPS_PK  ORDER BY 
            src:PPS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PMPLANSESSION as strm))