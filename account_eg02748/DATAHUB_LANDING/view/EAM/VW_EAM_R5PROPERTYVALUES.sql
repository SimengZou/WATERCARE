CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PROPERTYVALUES AS SELECT
                        src:PRV_CLASS::varchar AS PRV_CLASS, 
                        src:PRV_CLASS_ORG::varchar AS PRV_CLASS_ORG, 
                        src:PRV_CODE::varchar AS PRV_CODE, 
                        src:PRV_CREATED::datetime AS PRV_CREATED, 
                        src:PRV_DVALUE::datetime AS PRV_DVALUE, 
                        src:PRV_LASTSAVED::datetime AS PRV_LASTSAVED, 
                        src:PRV_NOTUSED::varchar AS PRV_NOTUSED, 
                        src:PRV_NVALUE::numeric(38, 10) AS PRV_NVALUE, 
                        src:PRV_PROPERTY::varchar AS PRV_PROPERTY, 
                        src:PRV_RENTITY::varchar AS PRV_RENTITY, 
                        src:PRV_SEQNO::numeric(38, 10) AS PRV_SEQNO, 
                        src:PRV_UPDATECOUNT::numeric(38, 10) AS PRV_UPDATECOUNT, 
                        src:PRV_UPDATED::datetime AS PRV_UPDATED, 
                        src:PRV_VALUE::varchar AS PRV_VALUE, 
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
    
                        
                src:PRV_CLASS,
                src:PRV_CLASS_ORG,
                src:PRV_CODE,
                src:PRV_PROPERTY,
                src:PRV_RENTITY,
                src:PRV_SEQNO,
            src:PRV_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PRV_CLASS,
                src:PRV_CLASS_ORG,
                src:PRV_CODE,
                src:PRV_PROPERTY,
                src:PRV_RENTITY,
                src:PRV_SEQNO  ORDER BY 
            src:PRV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PROPERTYVALUES as strm))