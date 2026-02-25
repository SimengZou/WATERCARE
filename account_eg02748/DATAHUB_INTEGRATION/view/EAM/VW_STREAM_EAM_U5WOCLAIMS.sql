CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5WOCLAIMS AS SELECT
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
                        src:WCO_ACTIVITY::varchar AS WCO_ACTIVITY, 
                        src:WCO_ACTIVITY_DESC::varchar AS WCO_ACTIVITY_DESC, 
                        src:WCO_CHARGEDATE::datetime AS WCO_CHARGEDATE, 
                        src:WCO_COMMENTS::varchar AS WCO_COMMENTS, 
                        src:WCO_COMMENTS_INT::varchar AS WCO_COMMENTS_INT, 
                        src:WCO_CONTRACTOR::varchar AS WCO_CONTRACTOR, 
                        src:WCO_CONTRACTOR_QTY::numeric(38, 10) AS WCO_CONTRACTOR_QTY, 
                        src:WCO_CONTRACTOR_RATE::numeric(38, 10) AS WCO_CONTRACTOR_RATE, 
                        src:WCO_CONTRACT_CODE::varchar AS WCO_CONTRACT_CODE, 
                        src:WCO_DERPROJACT::varchar AS WCO_DERPROJACT, 
                        src:WCO_DERPROJNUM::varchar AS WCO_DERPROJNUM, 
                        src:WCO_EVENT::varchar AS WCO_EVENT, 
                        src:WCO_EXPPROJACT::varchar AS WCO_EXPPROJACT, 
                        src:WCO_EXPPROJNUM::varchar AS WCO_EXPPROJNUM, 
                        src:WCO_LINETOTAL::numeric(38, 10) AS WCO_LINETOTAL, 
                        src:WCO_LINE_STATUS::varchar AS WCO_LINE_STATUS, 
                        src:WCO_ORG::varchar AS WCO_ORG, 
                        src:WCO_PK::varchar AS WCO_PK, 
                        src:WCO_PROCESSED_ERROR::varchar AS WCO_PROCESSED_ERROR, 
                        src:WCO_PROCESSED_STATUS::varchar AS WCO_PROCESSED_STATUS, 
                        src:WCO_SCHEDITEM_DESC::varchar AS WCO_SCHEDITEM_DESC, 
                        src:WCO_SCHEDITEM_RATE::numeric(38, 10) AS WCO_SCHEDITEM_RATE, 
                        src:WCO_SCHEDULE_ITEM::varchar AS WCO_SCHEDULE_ITEM, 
                        src:WCO_TRANSID::varchar AS WCO_TRANSID, 
                        src:WCO_TRANS_FLAG::varchar AS WCO_TRANS_FLAG, 
                        src:WCO_WOPARENT::varchar AS WCO_WOPARENT, 
                        src:WCO_WOSCHEDITEM::varchar AS WCO_WOSCHEDITEM, 
                        src:WCO_WOTYPE::varchar AS WCO_WOTYPE, 
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
                                        
                src:WCO_EVENT,
                src:WCO_ORG,
                src:WCO_PK  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5WOCLAIMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WCO_CHARGEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WCO_CONTRACTOR_QTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WCO_CONTRACTOR_RATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WCO_LINETOTAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WCO_SCHEDITEM_RATE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null