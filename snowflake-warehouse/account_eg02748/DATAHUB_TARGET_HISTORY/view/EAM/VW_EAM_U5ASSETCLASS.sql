
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5ASSETCLASS
                   as                       
                    SELECT
                        t.DESCRIPTION,
                        t.ASCL_CODE,
                        t.ASCL_FUNCGROUP,
                        t.ASCL_IPSCOMPTYPE,
                        t.ASCL_NOTUSED,
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
          
                     FROM DATAHUB_TARGET.EAM_U5ASSETCLASS as  t
					 union
					        SELECT
                        th.DESCRIPTION,
                        th.ASCL_CODE,
                        th.ASCL_FUNCGROUP,
                        th.ASCL_IPSCOMPTYPE,
                        th.ASCL_NOTUSED,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5ASSETCLASS as  th ;
                     