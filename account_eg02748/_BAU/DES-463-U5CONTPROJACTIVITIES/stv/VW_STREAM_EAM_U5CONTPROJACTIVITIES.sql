CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CONTPROJACTIVITIES AS
                    SELECT
                        src:"CSA_ORG"::varchar AS CSA_ORG,
                        src:"CSA_SCHEDULEITEM"::varchar AS CSA_SCHEDULEITEM,
                        src:"CSA_PROJECT"::varchar AS CSA_PROJECT,
                        src:"CSA_ACTIVITY"::varchar AS CSA_ACTIVITY,
                        src:"CSA_CONTRACTCODE"::varchar AS CSA_CONTRACTCODE,
                        src:"CSA_SUPPLIER"::varchar AS CSA_SUPPLIER,
						src:"CSA_CONTRACT"::varchar AS CSA_CONTRACT,
						src:"CSA_CONTRACTOR"::varchar AS CSA_CONTRACTOR,
                        src:"CREATEDBY"::varchar AS CREATEDBY,
                        src:"UPDATEDBY"::varchar AS UPDATEDBY,
                        src:"UPDATED"::datetime AS UPDATED,
                        src:"CREATED"::datetime AS CREATED,
                        src:"UPDATECOUNT"::numeric(38, 0) AS UPDATECOUNT,
                        src:"LASTSAVED"::datetime AS LASTSAVED
                        ,src:"LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTHISTORY;