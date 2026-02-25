
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILEDBSYNC
                   as                       
                    SELECT
                        t.MDS_SYNCID,
                        t.MDS_USER,
                        t.MDS_ORG,
                        t.MDS_STATUS,
                        t.MDS_DBLASTUPDATEDDATE,
                        t.MDS_GRIDS_PROCESSED,
                        t.MDS_STATUS_MSG,
                        t.MDS_SYNC_REQUEST,
                        t.MDS_DOWNLOAD,
                        t.MDS_FILENAME,
                        t.MDS_UPDATECOUNT,
                        t.MDS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILEDBSYNC as  t
					 union
					        SELECT
                        th.MDS_SYNCID,
                        th.MDS_USER,
                        th.MDS_ORG,
                        th.MDS_STATUS,
                        th.MDS_DBLASTUPDATEDDATE,
                        th.MDS_GRIDS_PROCESSED,
                        th.MDS_STATUS_MSG,
                        th.MDS_SYNC_REQUEST,
                        th.MDS_DOWNLOAD,
                        th.MDS_FILENAME,
                        th.MDS_UPDATECOUNT,
                        th.MDS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILEDBSYNC as  th ;
                     