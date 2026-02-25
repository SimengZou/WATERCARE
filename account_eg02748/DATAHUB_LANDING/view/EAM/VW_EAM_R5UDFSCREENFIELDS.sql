CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5UDFSCREENFIELDS AS SELECT
                        src:USF_COMPUTEDDATA::varchar AS USF_COMPUTEDDATA, 
                        src:USF_CREATED::datetime AS USF_CREATED, 
                        src:USF_CREATEDBY::varchar AS USF_CREATEDBY, 
                        src:USF_DESC::varchar AS USF_DESC, 
                        src:USF_DROPPDOWN::varchar AS USF_DROPPDOWN, 
                        src:USF_FIELDLABEL::varchar AS USF_FIELDLABEL, 
                        src:USF_FIELDNAME::varchar AS USF_FIELDNAME, 
                        src:USF_FIELDTYPE::varchar AS USF_FIELDTYPE, 
                        src:USF_GENERATED::varchar AS USF_GENERATED, 
                        src:USF_LASTSAVED::datetime AS USF_LASTSAVED, 
                        src:USF_LOOKUPQUERY::varchar AS USF_LOOKUPQUERY, 
                        src:USF_LOOKUPSOURCE::varchar AS USF_LOOKUPSOURCE, 
                        src:USF_MAXLENGTH::numeric(38, 10) AS USF_MAXLENGTH, 
                        src:USF_NOTUSED::varchar AS USF_NOTUSED, 
                        src:USF_NULLABLE::varchar AS USF_NULLABLE, 
                        src:USF_PARENTFIELD::varchar AS USF_PARENTFIELD, 
                        src:USF_PRECISION::numeric(38, 10) AS USF_PRECISION, 
                        src:USF_PRIMARY::varchar AS USF_PRIMARY, 
                        src:USF_RETRIEVEDVALUELOOKUP::varchar AS USF_RETRIEVEDVALUELOOKUP, 
                        src:USF_SCALE::numeric(38, 10) AS USF_SCALE, 
                        src:USF_SCREENNAME::varchar AS USF_SCREENNAME, 
                        src:USF_SEQUENCE::numeric(38, 10) AS USF_SEQUENCE, 
                        src:USF_UPDATECOUNT::numeric(38, 10) AS USF_UPDATECOUNT, 
                        src:USF_UPDATED::datetime AS USF_UPDATED, 
                        src:USF_UPDATEDBY::varchar AS USF_UPDATEDBY, 
                        src:USF_UPPERCASE::varchar AS USF_UPPERCASE, 
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
    
                        
                src:USF_FIELDNAME,
                src:USF_SCREENNAME,
            src:USF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:USF_FIELDNAME,
                src:USF_SCREENNAME  ORDER BY 
            src:USF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5UDFSCREENFIELDS as strm))