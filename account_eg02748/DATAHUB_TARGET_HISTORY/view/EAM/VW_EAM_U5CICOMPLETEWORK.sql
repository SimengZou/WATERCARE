
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5CICOMPLETEWORK
                   as                       
                    SELECT
                        t.CIC_TYPE,
                        t.CIC_TRANSID,
                        t.CIC_REFERENCE,
                        t.CIC_CONTRACTORCODE,
                        t.CIC_OBJECT,
                        t.CIC_COMMENT,
                        t.CIC_ACTIVITYCODE,
                        t.CIC_ASSETCONDITION,
                        t.CIC_CREW,
                        t.CIC_INITIATEDDATE,
                        t.CIC_STARTDATE,
                        t.CIC_COMPLETEDDATE,
                        t.CIC_RESULTCODE,
                        t.CIC_VARIATIONNO,
                        t.CIC_EVENT,
                        t.CIC_RESULTWONUM,
                        t.CIC_COMPLETEBY,
                        t.CIC_RESULTS,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5CICOMPLETEWORK as  t
					 union
					        SELECT
                        th.CIC_TYPE,
                        th.CIC_TRANSID,
                        th.CIC_REFERENCE,
                        th.CIC_CONTRACTORCODE,
                        th.CIC_OBJECT,
                        th.CIC_COMMENT,
                        th.CIC_ACTIVITYCODE,
                        th.CIC_ASSETCONDITION,
                        th.CIC_CREW,
                        th.CIC_INITIATEDDATE,
                        th.CIC_STARTDATE,
                        th.CIC_COMPLETEDDATE,
                        th.CIC_RESULTCODE,
                        th.CIC_VARIATIONNO,
                        th.CIC_EVENT,
                        th.CIC_RESULTWONUM,
                        th.CIC_COMPLETEBY,
                        th.CIC_RESULTS,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CICOMPLETEWORK as  th ;
                     