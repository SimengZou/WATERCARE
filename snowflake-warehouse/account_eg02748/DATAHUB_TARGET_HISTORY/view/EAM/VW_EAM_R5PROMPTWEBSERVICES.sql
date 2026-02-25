
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PROMPTWEBSERVICES
                   as                       
                    SELECT
                        t.PWS_PROCESSGROUP,
                        t.PWS_WSPRMCODE,
                        t.PWS_FUNCTION,
                        t.PWS_TAB,
                        t.PWS_ACTIONCODE,
                        t.PWS_WEBSERVICE,
                        t.PWS_ORGXPATH,
                        t.PWS_UPDATED,
                        t.PWS_UPDATECOUNT,
                        t.PWS_WSTITLE,
                        t.PWS_CFKEYCODE,
                        t.PWS_TOPBLOCKTITLE,
                        t.PWS_BOTTOMBLOCKTITLE,
                        t.PWS_CFBLOCKTITLE,
                        t.PWS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PROMPTWEBSERVICES as  t
					 union
					        SELECT
                        th.PWS_PROCESSGROUP,
                        th.PWS_WSPRMCODE,
                        th.PWS_FUNCTION,
                        th.PWS_TAB,
                        th.PWS_ACTIONCODE,
                        th.PWS_WEBSERVICE,
                        th.PWS_ORGXPATH,
                        th.PWS_UPDATED,
                        th.PWS_UPDATECOUNT,
                        th.PWS_WSTITLE,
                        th.PWS_CFKEYCODE,
                        th.PWS_TOPBLOCKTITLE,
                        th.PWS_BOTTOMBLOCKTITLE,
                        th.PWS_CFBLOCKTITLE,
                        th.PWS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PROMPTWEBSERVICES as  th ;
                     