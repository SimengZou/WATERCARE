
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5HYPERLINK
                   as                       
                    SELECT
                        t.HYP_SOURCEPAGENAME,
                        t.HYP_SOURCEELEMENTID,
                        t.HYP_DESTPAGENAME,
                        t.HYP_DESTELEMENTID,
                        t.HYP_SCREENMODE,
                        t.HYP_DATASPY,
                        t.HYP_UPDATECOUNT,
                        t.HYP_SRCLINENUMBER,
                        t.HYP_LINKNAME,
                        t.HYP_PK,
                        t.HYP_LASTSAVED,
                        t.HYP_PERFORMEXACTQUERY,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5HYPERLINK as  t
					 union
					        SELECT
                        th.HYP_SOURCEPAGENAME,
                        th.HYP_SOURCEELEMENTID,
                        th.HYP_DESTPAGENAME,
                        th.HYP_DESTELEMENTID,
                        th.HYP_SCREENMODE,
                        th.HYP_DATASPY,
                        th.HYP_UPDATECOUNT,
                        th.HYP_SRCLINENUMBER,
                        th.HYP_LINKNAME,
                        th.HYP_PK,
                        th.HYP_LASTSAVED,
                        th.HYP_PERFORMEXACTQUERY,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5HYPERLINK as  th ;
                     