
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CODESTRUCTURE
                   as                       
                    SELECT
                        t.CSR_CODE,
                        t.CSR_DESC,
                        t.CSR_RENTITY,
                        t.CSR_ENTITY,
                        t.CSR_STRLEVEL1,
                        t.CSR_STRLEVEL2,
                        t.CSR_STRLEVEL3,
                        t.CSR_STRLEVEL4,
                        t.CSR_STRLEVEL5,
                        t.CSR_STRLEVEL6,
                        t.CSR_STRLEVEL7,
                        t.CSR_STRLEVEL8,
                        t.CSR_STRUCTURE,
                        t.CSR_UPDATECOUNT,
                        t.CSR_UPDATED,
                        t.CSR_ICON,
                        t.CSR_ICONPATH,
                        t.CSR_IMPORTANCE,
                        t.CSR_MATERIALTYPE,
                        t.CSR_LASTSAVED,
                        t.CSR_NOTUSED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CODESTRUCTURE as  t
					 union
					        SELECT
                        th.CSR_CODE,
                        th.CSR_DESC,
                        th.CSR_RENTITY,
                        th.CSR_ENTITY,
                        th.CSR_STRLEVEL1,
                        th.CSR_STRLEVEL2,
                        th.CSR_STRLEVEL3,
                        th.CSR_STRLEVEL4,
                        th.CSR_STRLEVEL5,
                        th.CSR_STRLEVEL6,
                        th.CSR_STRLEVEL7,
                        th.CSR_STRLEVEL8,
                        th.CSR_STRUCTURE,
                        th.CSR_UPDATECOUNT,
                        th.CSR_UPDATED,
                        th.CSR_ICON,
                        th.CSR_ICONPATH,
                        th.CSR_IMPORTANCE,
                        th.CSR_MATERIALTYPE,
                        th.CSR_LASTSAVED,
                        th.CSR_NOTUSED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CODESTRUCTURE as  th ;
                     