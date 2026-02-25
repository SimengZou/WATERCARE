CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WARCOVERAGES AS SELECT
                        src:WCV_ACTIVE::varchar AS WCV_ACTIVE, 
                        src:WCV_CREATED::datetime AS WCV_CREATED, 
                        src:WCV_DURATION::numeric(38, 10) AS WCV_DURATION, 
                        src:WCV_EXPIRATION::numeric(38, 10) AS WCV_EXPIRATION, 
                        src:WCV_EXPIRATIONDATE::datetime AS WCV_EXPIRATIONDATE, 
                        src:WCV_LASTSAVED::datetime AS WCV_LASTSAVED, 
                        src:WCV_NEARTHRESHOLD::numeric(38, 10) AS WCV_NEARTHRESHOLD, 
                        src:WCV_OBJECT::varchar AS WCV_OBJECT, 
                        src:WCV_OBJECT_ORG::varchar AS WCV_OBJECT_ORG, 
                        src:WCV_OBRTYPE::varchar AS WCV_OBRTYPE, 
                        src:WCV_OBTYPE::varchar AS WCV_OBTYPE, 
                        src:WCV_RECORDED::datetime AS WCV_RECORDED, 
                        src:WCV_SEQNO::numeric(38, 10) AS WCV_SEQNO, 
                        src:WCV_STARTDATE::datetime AS WCV_STARTDATE, 
                        src:WCV_STARTUSAGE::numeric(38, 10) AS WCV_STARTUSAGE, 
                        src:WCV_UOM::varchar AS WCV_UOM, 
                        src:WCV_UPDATECOUNT::numeric(38, 10) AS WCV_UPDATECOUNT, 
                        src:WCV_UPDATED::datetime AS WCV_UPDATED, 
                        src:WCV_USER::varchar AS WCV_USER, 
                        src:WCV_WARRANTY::varchar AS WCV_WARRANTY, 
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
    
                        
                src:WCV_SEQNO,
            src:WCV_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WCV_SEQNO  ORDER BY 
            src:WCV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WARCOVERAGES as strm))