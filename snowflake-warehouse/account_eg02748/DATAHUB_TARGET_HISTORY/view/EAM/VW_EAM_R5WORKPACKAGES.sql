
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WORKPACKAGES
                   as                       
                    SELECT
                        t.WPK_CODE,
                        t.WPK_DESC,
                        t.WPK_ORG,
                        t.WPK_JOBTYPE,
                        t.WPK_STATUS,
                        t.WPK_MRC,
                        t.WPK_CLASS,
                        t.WPK_CLASS_ORG,
                        t.WPK_JOBCLASS,
                        t.WPK_JOBCLASS_ORG,
                        t.WPK_PPMTYPE,
                        t.WPK_TRADE,
                        t.WPK_OBJECT,
                        t.WPK_OBJECT_ORG,
                        t.WPK_LASTEVENT,
                        t.WPK_DUEDATE,
                        t.WPK_FREQ,
                        t.WPK_DURATION,
                        t.WPK_ESTWORKLOAD,
                        t.WPK_PERSONS,
                        t.WPK_CHANGED,
                        t.WPK_PERIODUOM,
                        t.WPK_UPDATECOUNT,
                        t.WPK_PERFORMONWEEK,
                        t.WPK_PERFORMONDAY,
                        t.WPK_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WORKPACKAGES as  t
					 union
					        SELECT
                        th.WPK_CODE,
                        th.WPK_DESC,
                        th.WPK_ORG,
                        th.WPK_JOBTYPE,
                        th.WPK_STATUS,
                        th.WPK_MRC,
                        th.WPK_CLASS,
                        th.WPK_CLASS_ORG,
                        th.WPK_JOBCLASS,
                        th.WPK_JOBCLASS_ORG,
                        th.WPK_PPMTYPE,
                        th.WPK_TRADE,
                        th.WPK_OBJECT,
                        th.WPK_OBJECT_ORG,
                        th.WPK_LASTEVENT,
                        th.WPK_DUEDATE,
                        th.WPK_FREQ,
                        th.WPK_DURATION,
                        th.WPK_ESTWORKLOAD,
                        th.WPK_PERSONS,
                        th.WPK_CHANGED,
                        th.WPK_PERIODUOM,
                        th.WPK_UPDATECOUNT,
                        th.WPK_PERFORMONWEEK,
                        th.WPK_PERFORMONDAY,
                        th.WPK_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WORKPACKAGES as  th ;
                     