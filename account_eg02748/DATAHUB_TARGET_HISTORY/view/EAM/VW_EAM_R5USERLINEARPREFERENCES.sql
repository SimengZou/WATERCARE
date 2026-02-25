
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERLINEARPREFERENCES
                   as                       
                    SELECT
                        t.ULP_USERCODE,
                        t.ULP_LOLINEARREF,
                        t.ULP_LOPOINTSOFINT,
                        t.ULP_LORELATEDEQ,
                        t.ULP_LORELATEDPART,
                        t.ULP_LODEFAULTCOLOR,
                        t.ULP_LODEFAULTFILTER,
                        t.ULP_LOINCLUDERELATED,
                        t.ULP_CREATED,
                        t.ULP_CREATEDBY,
                        t.ULP_UPDATED,
                        t.ULP_UPDATEDBY,
                        t.ULP_UPDATECOUNT,
                        t.ULP_LOROUTEANDSEGMENT,
                        t.ULP_CODE,
                        t.ULP_DESC,
                        t.ULP_DEFAULT,
                        t.ULP_HIDESEGMENTS,
                        t.ULP_HIDEROUES,
                        t.ULP_GROUPSEGMENTS,
                        t.ULP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERLINEARPREFERENCES as  t
					 union
					        SELECT
                        th.ULP_USERCODE,
                        th.ULP_LOLINEARREF,
                        th.ULP_LOPOINTSOFINT,
                        th.ULP_LORELATEDEQ,
                        th.ULP_LORELATEDPART,
                        th.ULP_LODEFAULTCOLOR,
                        th.ULP_LODEFAULTFILTER,
                        th.ULP_LOINCLUDERELATED,
                        th.ULP_CREATED,
                        th.ULP_CREATEDBY,
                        th.ULP_UPDATED,
                        th.ULP_UPDATEDBY,
                        th.ULP_UPDATECOUNT,
                        th.ULP_LOROUTEANDSEGMENT,
                        th.ULP_CODE,
                        th.ULP_DESC,
                        th.ULP_DEFAULT,
                        th.ULP_HIDESEGMENTS,
                        th.ULP_HIDEROUES,
                        th.ULP_GROUPSEGMENTS,
                        th.ULP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERLINEARPREFERENCES as  th ;
                     