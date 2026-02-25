
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERTABCOLUMNS
                   as                       
                    SELECT
                        t.UTC_TABLENAME,
                        t.UTC_COLUMNNAME,
                        t.UTC_DATATYPE,
                        t.UTC_OBJ_XTYPE,
                        t.UTC_COL_XTYPE,
                        t.UTC_COLLATION,
                        t.UTC_ISID,
                        t.UTC_DATABASE,
                        t.UTC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERTABCOLUMNS as  t
					 union
					        SELECT
                        th.UTC_TABLENAME,
                        th.UTC_COLUMNNAME,
                        th.UTC_DATATYPE,
                        th.UTC_OBJ_XTYPE,
                        th.UTC_COL_XTYPE,
                        th.UTC_COLLATION,
                        th.UTC_ISID,
                        th.UTC_DATABASE,
                        th.UTC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERTABCOLUMNS as  th ;
                     