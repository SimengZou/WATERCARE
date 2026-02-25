CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5NONCONFORMITYSETUP AS SELECT
                        src:NCP_AUTOAPPROVESTATUS::varchar AS NCP_AUTOAPPROVESTATUS, 
                        src:NCP_AUTOSKIPSTATUS::varchar AS NCP_AUTOSKIPSTATUS, 
                        src:NCP_COPYFROM::varchar AS NCP_COPYFROM, 
                        src:NCP_COPYIMPORTANCE::varchar AS NCP_COPYIMPORTANCE, 
                        src:NCP_COPYINTENSITY::varchar AS NCP_COPYINTENSITY, 
                        src:NCP_COPYNEXTINSPDATEOVERRIDE::varchar AS NCP_COPYNEXTINSPDATEOVERRIDE, 
                        src:NCP_COPYOBSERVATIONUDFS::varchar AS NCP_COPYOBSERVATIONUDFS, 
                        src:NCP_COPYSEVERITY::varchar AS NCP_COPYSEVERITY, 
                        src:NCP_COPYSIZE::varchar AS NCP_COPYSIZE, 
                        src:NCP_CREATEFROMCHECKLIST::varchar AS NCP_CREATEFROMCHECKLIST, 
                        src:NCP_INCLUDECONDITION::varchar AS NCP_INCLUDECONDITION, 
                        src:NCP_LASTSAVED::datetime AS NCP_LASTSAVED, 
                        src:NCP_MASSACKNOWLEDGEALLOWED::varchar AS NCP_MASSACKNOWLEDGEALLOWED, 
                        src:NCP_MERGERESTRICTIONS::varchar AS NCP_MERGERESTRICTIONS, 
                        src:NCP_ORG::varchar AS NCP_ORG, 
                        src:NCP_PREVOBSCOUNT::numeric(38, 10) AS NCP_PREVOBSCOUNT, 
                        src:NCP_PROTECTHEADER::varchar AS NCP_PROTECTHEADER, 
                        src:NCP_PROTECTOBSDATAONHDR::varchar AS NCP_PROTECTOBSDATAONHDR, 
                        src:NCP_REINSPECTSTATUS::varchar AS NCP_REINSPECTSTATUS, 
                        src:NCP_SUPERSEDEDSTATUS::varchar AS NCP_SUPERSEDEDSTATUS, 
                        src:NCP_SYNCHRONIZE::varchar AS NCP_SYNCHRONIZE, 
                        src:NCP_UPDATECOUNT::numeric(38, 10) AS NCP_UPDATECOUNT, 
                        src:NCP_UPDATED::datetime AS NCP_UPDATED, 
                        src:NCP_UPDATEDBY::varchar AS NCP_UPDATEDBY, 
                        src:NCP_USEOBSSTATUS::varchar AS NCP_USEOBSSTATUS, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:NCP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:NCP_ORG  ORDER BY 
            src:NCP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5NONCONFORMITYSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NCP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NCP_PREVOBSCOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NCP_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NCP_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NCP_LASTSAVED), '1900-01-01')) is not null