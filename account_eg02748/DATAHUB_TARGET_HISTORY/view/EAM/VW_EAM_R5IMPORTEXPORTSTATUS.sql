
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IMPORTEXPORTSTATUS
                   as                       
                    SELECT
                        t.IES_USERID,
                        t.IES_SESSIONID,
                        t.IES_TYPE,
                        t.IES_STATUS,
                        t.IES_FILELOCATION,
                        t.IES_EMAILSENT,
                        t.IES_FILESENT,
                        t.IES_ESTTIMETOSTART,
                        t.IES_ESTTIMETOEND,
                        t.IES_DATECREATED,
                        t.IES_UPDATECOUNT,
                        t.IES_STARTED,
                        t.IES_COMPLETED,
                        t.IES_RECORDCOUNT,
                        t.IES_LASTSAVED,
                        t.IES_EMAIL,
                        t.IES_DESC,
                        t.IES_INCLUDEEMAIL,
                        t.IES_PREVIEW,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IMPORTEXPORTSTATUS as  t
					 union
					        SELECT
                        th.IES_USERID,
                        th.IES_SESSIONID,
                        th.IES_TYPE,
                        th.IES_STATUS,
                        th.IES_FILELOCATION,
                        th.IES_EMAILSENT,
                        th.IES_FILESENT,
                        th.IES_ESTTIMETOSTART,
                        th.IES_ESTTIMETOEND,
                        th.IES_DATECREATED,
                        th.IES_UPDATECOUNT,
                        th.IES_STARTED,
                        th.IES_COMPLETED,
                        th.IES_RECORDCOUNT,
                        th.IES_LASTSAVED,
                        th.IES_EMAIL,
                        th.IES_DESC,
                        th.IES_INCLUDEEMAIL,
                        th.IES_PREVIEW,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IMPORTEXPORTSTATUS as  th ;
                     