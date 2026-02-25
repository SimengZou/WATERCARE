
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IESREPOSITORIES
                   as                       
                    SELECT
                        t.ENS_CODE,
                        t.ENS_DESC,
                        t.ENS_TABLENAME,
                        t.ENS_USERFUNCTION,
                        t.ENS_TAB,
                        t.ENS_THUMBNAIL,
                        t.ENS_DATECREATEDCOL,
                        t.ENS_DATEUPDATEDCOL,
                        t.ENS_ORGCOL,
                        t.ENS_TABLEPREFIX,
                        t.ENS_INTERESTCENTER,
                        t.ENS_NOTUSED,
                        t.ENS_ISINCREMENTAL,
                        t.ENS_ISPOPUP,
                        t.ENS_DATELASTCRAWL,
                        t.ENS_UPDATECOUNT,
                        t.ENS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IESREPOSITORIES as  t
					 union
					        SELECT
                        th.ENS_CODE,
                        th.ENS_DESC,
                        th.ENS_TABLENAME,
                        th.ENS_USERFUNCTION,
                        th.ENS_TAB,
                        th.ENS_THUMBNAIL,
                        th.ENS_DATECREATEDCOL,
                        th.ENS_DATEUPDATEDCOL,
                        th.ENS_ORGCOL,
                        th.ENS_TABLEPREFIX,
                        th.ENS_INTERESTCENTER,
                        th.ENS_NOTUSED,
                        th.ENS_ISINCREMENTAL,
                        th.ENS_ISPOPUP,
                        th.ENS_DATELASTCRAWL,
                        th.ENS_UPDATECOUNT,
                        th.ENS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IESREPOSITORIES as  th ;
                     