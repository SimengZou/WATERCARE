CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CICOMPLETEWORK AS SELECT
                        src:CIC_ACTIVITYCODE::varchar AS CIC_ACTIVITYCODE, 
                        src:CIC_ASSETCONDITION::varchar AS CIC_ASSETCONDITION, 
                        src:CIC_COMMENT::varchar AS CIC_COMMENT, 
                        src:CIC_COMPLETEBY::varchar AS CIC_COMPLETEBY, 
                        src:CIC_COMPLETEDDATE::datetime AS CIC_COMPLETEDDATE, 
                        src:CIC_CONTRACTORCODE::varchar AS CIC_CONTRACTORCODE, 
                        src:CIC_CREW::varchar AS CIC_CREW, 
                        src:CIC_EVENT::varchar AS CIC_EVENT, 
                        src:CIC_INITIATEDDATE::datetime AS CIC_INITIATEDDATE, 
                        src:CIC_OBJECT::varchar AS CIC_OBJECT, 
                        src:CIC_REFERENCE::varchar AS CIC_REFERENCE, 
                        src:CIC_RESULTCODE::varchar AS CIC_RESULTCODE, 
                        src:CIC_RESULTS::varchar AS CIC_RESULTS, 
                        src:CIC_RESULTWONUM::varchar AS CIC_RESULTWONUM, 
                        src:CIC_STARTDATE::datetime AS CIC_STARTDATE, 
                        src:CIC_TRANSID::varchar AS CIC_TRANSID, 
                        src:CIC_TYPE::varchar AS CIC_TYPE, 
                        src:CIC_VARIATIONNO::varchar AS CIC_VARIATIONNO, 
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CIC_TRANSID  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5CICOMPLETEWORK as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CIC_COMPLETEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CIC_INITIATEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CIC_STARTDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null