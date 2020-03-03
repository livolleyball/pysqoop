### 使用方法

#### 环境
* python3 pymysql/retrying
* sqoop-cdh
* mysql 建表语句 dmp_data_source 
```sql
CREATE TABLE `dmp_data_source` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `db_name` varchar(50) NOT NULL COMMENT '数据库名称',
  `type` varchar(10) NOT NULL COMMENT '读写类别read/write',
  `connect` varchar(500) NOT NULL COMMENT 'jdbc地址',
  `user_name` varchar(100) NOT NULL COMMENT '数据库用户',
  `passwd` varchar(200) NOT NULL,
  `db_type` varchar(30) NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dmp_data_source_id_uindex` (`id`),
  UNIQUE KEY `uqk_db` (`db_name`,`type`,`db_type`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='数据源配置信息'
```

#### mysql2hive
##### mysql-full
```shell
dstDb_Table=oyo_tmp.hera_job_2221
mapper_nums=1
srcDb_Table=rdfa_datastudio.hera_job

res_query="select * ,now() as etl_time from  rdfa_datastudio.hera_job WHERE \$CONDITIONS "
python3 app.py  mysql-full \
${srcDb_Table} \
${dstDb_Table} \
"${res_query}" \
${mapper_nums}

```


##### mysql-incre
```shell
dstDb_Table=oyo_tmp.hera_job_15
mapper_nums=1
srcDb_Table=hera.hera_job

res_query="
select  id,configs,gmt_modified,now() as etl_time
from hera.hera_job
WHERE \$CONDITIONS 
and gmt_modified>=\"${zdt.addDay(-1).add(11,-1).format("yyyy-MM-dd HH:mm:ss")}\" "


python3 /u01/hera/app/py_sqoop/app.py  mysql-incre \
${srcDb_Table} \
${dstDb_Table} \
"${res_query}" \
${mapper_nums}

```


##### mysql-incre-par
```shell
dstDb_Table=oyo_tmp.hera_job_di
mapper_nums=1
srcDb_Table=hera.hera_job



res_query="
select  id,configs,now() as etl_time
from hera.hera_job
WHERE \$CONDITIONS 
and gmt_modified>=\"${zdt.addDay(-1).add(11,-1).format("yyyy-MM-dd HH:mm:ss")}\" "


partition_key=dt
partition_value=${zdt.addDay(-1).format("yyyy-MM-dd")}

python3 /u01/hera/app/py_sqoop/app.py  mysql-incre-par ${srcDb_Table} \
${dstDb_Table} \
"${res_query}" \
${mapper_nums} \
${partition_key} \
${partition_value}
```



#### hive2mysql:

##### 删除后写入
```shell

dstDb_Table=hera.lihm_20190812
dstDb_Table_columns=member_id,perfer_citys,live_city,base_city,sync_time
mapper_nums=2
srcDb_Table=dw.dw_datacenter_new_member_dim_dw

res_query="select member_id,perfer_citys,live_city,base_city,sync_time from dw.dw_datacenter_new_member_dim_dw limit 1000 "

pre_sql="DELETE FROM hera.lihm_20190812 WHERE id < 10"

python3 app.py load-mysql \
${srcDb_Table} \
${dstDb_Table} \
${dstDb_Table_columns} \
"${res_query}" \
${mapper_nums} \
"${pre_sql}"

```


##### upsert 更新并插入
```shell
dstDb_Table=hera.hera_job_lihm
mapper_nums=1
srcDb_Table=oyo_ods.ods_hera_hera_job
update_key=id

res_query="select id
,auto
,'lihm03' as name 
from oyo_ods.ods_hera_hera_job "

python3 app.py load-mysql-upsert \
${srcDb_Table} \
${dstDb_Table} \
${update_key} \
"${res_query}" \
${mapper_nums} 
```

