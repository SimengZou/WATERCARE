CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5RELIABILITYRANKINGRANKS AS SELECT
                        src:RRR_COLOR::varchar AS RRR_COLOR, 
                        src:RRR_CRITICALITY::varchar AS RRR_CRITICALITY, 
                        src:RRR_ICON::varchar AS RRR_ICON, 
                        src:RRR_ICONPATH::varchar AS RRR_ICONPATH, 
                        src:RRR_LASTSAVED::datetime AS RRR_LASTSAVED, 
                        src:RRR_MAXVALUE::numeric(38, 10) AS RRR_MAXVALUE, 
                        src:RRR_MINVALUE::numeric(38, 10) AS RRR_MINVALUE, 
                        src:RRR_ORG::varchar AS RRR_ORG, 
                        src:RRR_PK::varchar AS RRR_PK, 
                        src:RRR_RELIABILITYRANKING::varchar AS RRR_RELIABILITYRANKING, 
                        src:RRR_REVISION::numeric(38, 10) AS RRR_REVISION, 
                        src:RRR_RRINDEX::varchar AS RRR_RRINDEX, 
                        src:RRR_UPDATECOUNT::numeric(38, 10) AS RRR_UPDATECOUNT, 
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
    
                        
                src:RRR_PK,
            src:RRR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RRR_PK  ORDER BY 
            src:RRR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5RELIABILITYRANKINGRANKS as strm))