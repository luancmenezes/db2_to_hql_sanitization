CREATE EXTERNAL TABLE IF NOT EXISTS BALP.PGDT212(
VL_MAXI_HORA_NAO DECIMAL(17 , 2),
QT_MAXI_HORA_NAO DATE,
VL_MAXI_HORA_UTIL DECIMAL(17 , 2),
QT_MAXI_HORA_UTIL DATE,
DH_ULTI_ATLZ DATESTAMP(6),
CD_TERN_ULTI_ATLZ string,
CD_USUA_ULTI_ATLZ string,
CD_CENT_ULTI_ATLZ string,
CD_EMPR_ULTI_ATLZ string,
VL_MAXI_DIA_NAO DECIMAL(17 , 2),
QT_MAXI_DIA_NAO DATE,
VL_MAXI_DIA_UTIL DECIMAL(17 , 2),
QT_MAXI_DIA_UTIL DATE,
DS_GRUP string,
HR_FIM DATE,
HR_INIC DATE,
CD_GRUP string,
),
 PARTITIONED BY (
 dat_ref_carga string
),
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED As INPUTFORMAT
 '	org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'	org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
TBLPROPERTIES ('parquet.compression'='SNAPPY');