
                         copy into DATAHUB_LANDING.LN_BPMDM001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_BPMDM001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_ENUM
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_ENUM(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCCP060
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCCP060(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCCP070
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCCP070(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM000
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM000(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM101
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM101(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM110
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM110(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM111
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM111(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM112
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM112(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM114
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM114(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM120
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM120(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM121
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM121(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM122
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM122(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM124
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM124(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM130
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM130(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM139
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM139(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCCOM161
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCCOM161(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCEMM030
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCEMM030(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCEMM050
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCEMM050(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCEMM100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCEMM100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCEMM135
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCEMM135(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCEMM170
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCEMM170(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCGEN000
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCGEN000(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCIBD001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCIBD001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCIBD301
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCIBD301(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCLCT020
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCLCT020(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS000
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS000(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS002
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS002(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS003
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS003(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS005
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS005(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS013
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS013(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS015
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS015(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS023
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS023(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS024
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS024(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS028
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS028(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS029
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS029(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS031
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS031(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS041
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS041(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS044
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS044(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS045
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS045(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS048
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS048(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS052
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS052(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS060
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS060(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS061
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS061(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS062
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS062(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS064
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS064(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS065
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS065(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS066
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS066(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS080
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS080(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS122
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS122(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS124
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS124(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS125
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS125(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS127
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS127(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS143
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS143(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCMCS145
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCMCS145(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCPPL040
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCPPL040(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TCPPL090
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TCPPL090(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDCMS050
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDCMS050(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDIPU001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDIPU001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDISA001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDISA001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR012
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR012(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR094
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR094(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR101
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR101(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR105
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR105(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR106
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR106(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR201
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR201(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR300
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR300(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR301
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR301(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR303
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR303(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR400
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR400(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR401
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR401(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR406
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR406(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR411
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR411(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR430
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR430(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDPUR456
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR456(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS094
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS094(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS300
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS300(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS301
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS301(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS307
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS307(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS311
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS311(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS400
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS400(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSLS401
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSLS401(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSMI003
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSMI003(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSMI007
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSMI007(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSMI008
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSMI008(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSMI110
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSMI110(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TDSMI113
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TDSMI113(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP240
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP240(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP245
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP245(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP251
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP251(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP256
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP256(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP260
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP260(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP270
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP270(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACP600
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACP600(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFACR001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFACR001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM110
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM110(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM120
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM120(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM400
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM400(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM500
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM500(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM510
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM510(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM600
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM600(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM650
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM650(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM700
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM700(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM710
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM710(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM800
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM800(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM805
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM805(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM807
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM807(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM808
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM808(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM810
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM810(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM820
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM820(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM830
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM830(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFAM840
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFAM840(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFBS001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFBS001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFBS003
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFBS003(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFBS005
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFBS005(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFBS100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFBS100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFFBS101
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFFBS101(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD003
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD003(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD004
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD004(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD005
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD005(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD006
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD006(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD007
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD007(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD008
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD008(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD106
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD106(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD170
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD170(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD171
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD171(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD172
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD172(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD201
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD201(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD202
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD202(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD203
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD203(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD204
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD204(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD205
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD205(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD206
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD206(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD482
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD482(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TFGLD495
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TFGLD495(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TICPR050
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TICPR050(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TICST001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TICST001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TICST002
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TICST002(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TICST010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TICST010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIIPD001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIIPD001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIPCF500
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIPCF500(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIPCS020
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIPCS020(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIPCS030
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIPCS030(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIROU001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIROU001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIROU002
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIROU002(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIROU003
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIROU003(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TIROU101
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TIROU101(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TISFC001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TISFC001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TISFC010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TISFC010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPCTM010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPCTM010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPCTM060
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPCTM060(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPCTM100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPCTM100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPCTM110
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPCTM110(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM000
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM000(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM001
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM001(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM002
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM002(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM005
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM005(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM007
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM007(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM015
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM015(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM016
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM016(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM025
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM025(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM035
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM035(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM040
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM040(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM042
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM042(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM043
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM043(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM045
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM045(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM046
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM046(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM048
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM048(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM055
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM055(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM059
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM059(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM063
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM063(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM065
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM065(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM075
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM075(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM085
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM085(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM095
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM095(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM096
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM096(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM110
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM110(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM600
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM600(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM601
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM601(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM605
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM605(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM615
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM615(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM625
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM625(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM635
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM635(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM640
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM640(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM643
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM643(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM646
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM646(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM649
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM649(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM700
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM700(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM790
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM790(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPDM801
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPDM801(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN011
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN011(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN020
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN020(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN040
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN040(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN050
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN050(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN060
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN060(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPIN080
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPIN080(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC000
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC000(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC160
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC160(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC205
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC205(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC211
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC211(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC216
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC216(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC231
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC231(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC232
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC232(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC236
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC236(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC250
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC250(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC251
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC251(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC256
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC256(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC271
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC271(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC276
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC276(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC291
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC291(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC296
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC296(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC301
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC301(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC305
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC305(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC350
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC350(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC400
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC400(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC401
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC401(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC402
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC402(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC410
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC410(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC411
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC411(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC413
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC413(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC414
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC414(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC420
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC420(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC440
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC440(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC441
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC441(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC442
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC442(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC450
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC450(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC451
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC451(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC453
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC453(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC454
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC454(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC470
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC470(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC600
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC600(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPPC606
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPPC606(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS020
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS020(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS202
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS202(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS210
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS210(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS220
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS220(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPSS600
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPSS600(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC050
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC050(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC101
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC101(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC120
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC120(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC127
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC127(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC130
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC130(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC137
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC137(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC140
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC140(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC147
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC147(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC150
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC150(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC157
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC157(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC160
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC160(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC220
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC220(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC230
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC230(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC240
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC240(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC250
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC250(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC300
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC300(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC311
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC311(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC315
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC315(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC316
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC316(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC350
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC350(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC351
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC351(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC352
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC352(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC353
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC353(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC354
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC354(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TPPTC510
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TPPTC510(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSBSC100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSBSC100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCFG200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCFG200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCLM100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCLM100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCLM330
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCLM330(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCLM335
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCLM335(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCLM350
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCLM350(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM110
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM110(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM111
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM111(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM120
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM120(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM130
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM130(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM131
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM131(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM132
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM132(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM135
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM135(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM136
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM136(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM139
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM139(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM300
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM300(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM320
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM320(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM400
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM400(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM450
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM450(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM460
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM460(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM480
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM480(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM500
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM500(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSCTM501
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSCTM501(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSMDM000
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSMDM000(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSMDM015
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSMDM015(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSMDM100
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSMDM100(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSMDM105
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSMDM105(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSMDM140
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSMDM140(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSMDM145
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSMDM145(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSSOC200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSSOC200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSSOC205
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSSOC205(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TSSOC210
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TSSOC210(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTAAD111
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTAAD111(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTAAD200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTAAD200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTAAD420
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTAAD420(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTAMS460
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTAMS460(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTDPM510
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTDPM510(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTDPM515
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTDPM515(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTDPM530
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTDPM530(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TTTXT010
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TTTXT010(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TXPDM901
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TXPDM901(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TXPDM902
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TXPDM902(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TXPDM904
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TXPDM904(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TXPDM905
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TXPDM905(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TXPDM907
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TXPDM907(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_TXPDM908
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_TXPDM908(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA112
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA112(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA113
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA113(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA114
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA114(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA115
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA115(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA116
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA116(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA117
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA117(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA118
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA118(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA122
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA122(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA123
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA123(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA124
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA124(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA126
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA126(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINA127
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINA127(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH210
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH210(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH220
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH220(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH310
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH310(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH312
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH312(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH430
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH430(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH431
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH431(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH435
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH435(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH439
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH439(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH440
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH440(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH500
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH500(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH501
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH501(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH520
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH520(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH521
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH521(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH540
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH540(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHINH600
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH600(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHWMD200
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHWMD200(FILE_FORMAT => json_format));
        
        
                         copy into DATAHUB_LANDING.LN_WHWMD300
                    from (
                            SELECT
                            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                            FROM @DATAHUB_INTEGRATION.STAGE_LN_WHWMD300(FILE_FORMAT => json_format));
        
        