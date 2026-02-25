CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMCONFIG AS SELECT
                        src:CLC_CHILDWORATEAPP::varchar AS CLC_CHILDWORATEAPP, 
                        src:CLC_CONTORDERTYPE::varchar AS CLC_CONTORDERTYPE, 
                        src:CLC_CONTRACTOR::varchar AS CLC_CONTRACTOR, 
                        src:CLC_COSTITEM::varchar AS CLC_COSTITEM, 
                        src:CLC_CREATOR::varchar AS CLC_CREATOR, 
                        src:CLC_EXCLUDEPOFROMLN::varchar AS CLC_EXCLUDEPOFROMLN, 
                        src:CLC_EXCLUDEWOFROMLN::varchar AS CLC_EXCLUDEWOFROMLN, 
                        src:CLC_LNCOMPANY::varchar AS CLC_LNCOMPANY, 
                        src:CLC_NONCONTORDERTYPE::varchar AS CLC_NONCONTORDERTYPE, 
                        src:CLC_ORDERSERIES::varchar AS CLC_ORDERSERIES, 
                        src:CLC_ORG::varchar AS CLC_ORG, 
                        src:CLC_PMWORATEAPP::varchar AS CLC_PMWORATEAPP, 
                        src:CLC_PROJECTACTIVITY::varchar AS CLC_PROJECTACTIVITY, 
                        src:CLC_PROJECTNUMBER::varchar AS CLC_PROJECTNUMBER, 
                        src:CLC_PURCHASEOFFICE::varchar AS CLC_PURCHASEOFFICE, 
                        src:CLC_REQUESTOR::varchar AS CLC_REQUESTOR, 
                        src:CLC_REQUESTOR2::varchar AS CLC_REQUESTOR2, 
                        src:CLC_SPLITINTERCOMPANYCOSTS::varchar AS CLC_SPLITINTERCOMPANYCOSTS, 
                        src:CLC_STCR_DIM3::varchar AS CLC_STCR_DIM3, 
                        src:CLC_STCR_DIM4::varchar AS CLC_STCR_DIM4, 
                        src:CLC_STGCOSTCODE::varchar AS CLC_STGCOSTCODE, 
                        src:CLC_STORE::varchar AS CLC_STORE, 
                        src:CLC_SUPPLIER::varchar AS CLC_SUPPLIER, 
                        src:CLC_TRANS_FLAG::varchar AS CLC_TRANS_FLAG, 
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
                                        
                src:CLC_CONTRACTOR,
                src:CLC_ORG  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5CLAIMCONFIG as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null