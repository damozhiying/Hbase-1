/*******************************c_cdn_index建表SQL*********************************/
DROP TABLE IF EXISTS c_cdn_index;
CREATE TABLE c_cdn_index_multiple_cloumn (
  flow_id DECIMAL(20) NOT NULL,
  A.collect_id integer(11) ,
  A.client_id VARCHAR(64) ,
  A.account VARCHAR(64) ,
  B.stb_ip VARCHAR(32) ,
  B.probe_id VARCHAR(10) ,
  B.index_id VARCHAR(16) ,
  B.index_val VARCHAR(64) ,
  start_time TIMESTAMP not null,
  M.end_time TIMESTAMP ,
  M.up_time TIMESTAMP ,
  M.deal_flag integer(11) ,
  M.ext_col VARCHAR(64) ,
  M.service_type integer(11) ,
  CONSTRAINT PK PRIMARY KEY (flow_id,start_time)
)
/*创建索引*/
CREATE INDEX IF NOT EXISTS INDEX_COLLECT_ID ON C_CDN_INDEX ( collect_id)
/*查询总数*/
select count(1) from "C_CDN_INDEX"
/*******************************client_info建表SQL*********************************/
-- 表优化
-- 1、加盐
-- 2、多列族
-- 3、COMPRESSION压缩
DROP TABLE IF EXISTS client_info;
CREATE TABLE client_info (
  flow_id DECIMAL(20) NOT NULL,
  CL1.client_id VARCHAR(64),
  CL1.account_code VARCHAR(64),
  CL1.probe_id VARCHAR(10),
  CL1.stb_ip VARCHAR(32),
  CL1.wlan_ip VARCHAR(32),
  CL2.wlan_number_ip DECIMAL(20),
  CL2.mac VARCHAR(32),
  CL2.mac2 VARCHAR(32),
  CL2.net_model VARCHAR(16),
  CL2.client_type VARCHAR(8),
  CL3.client_model VARCHAR(128),
  CL3.client_version VARCHAR(128),
  CL3.client_factory VARCHAR(16),
  CL3.client_state INTEGER(11),
  CL3.client_mem INTEGER(11),
  CL3.client_cpu VARCHAR(64),
  CL4.os_type VARCHAR(16),
  CL4.os_version VARCHAR(16),
  CL4.province VARCHAR(16),
  CL4.city VARCHAR(16),
  CL4.area VARCHAR(16),
  CL4.addr VARCHAR(256),
  CL5.parent_node VARCHAR(20),
  CL5.up_time VARCHAR(32),
  CL5.bd_account VARCHAR(32),
  CL5.is_legal VARCHAR(1),
  CL5.insert_time VARCHAR(32),
  CL5.opt_name VARCHAR(32),
  CL5.play_state INTEGER(11),
  CONSTRAINT pk PRIMARY KEY (flow_id)
)SALT_BUCKETS=16,COMPRESSION='GZ';
/**********************c_ott_qoe建表语句***************************/
-- 表优化
-- 1、加盐
-- 2、多列族
-- 3、COMPRESSION压缩
DROP TABLE IF EXISTS c_ott_qoe;
CREATE TABLE c_ott_qoe (
  flow_id DECIMAL(20) NOT NULL,
  CL1.collect_id INTEGER (11) ,
  CL1.client_id VARCHAR(64) ,
  CL1.account VARCHAR(64) ,
  CL1.stb_ip VARCHAR(32) ,
  CL2.probe_id VARCHAR(10) ,
  CL2.index_id VARCHAR(16) ,
  CL2.index_val VARCHAR(16) ,
  start_time VARCHAR(64) NOT NULL ,
  CL3.end_time VARCHAR(64) ,
  CL3.up_time VARCHAR(64) ,
  CL3.deal_flag INTEGER (11) ,
  CONSTRAINT pk PRIMARY KEY (flow_id,start_time)
)SALT_BUCKETS=16,COMPRESSION='GZ';
/*******************************res_link_relation建表SQL*********************************/
DROP TABLE IF EXISTS res_link_relation;
CREATE TABLE res_link_relation (
  flow_id integer(11) NOT NULL ,
  node_name VARCHAR(64) ,
  parent_node integer(11) ,
  node_type VARCHAR(20),
  node_ip VARCHAR(20) ,
  busi_type integer(11) ,
  group_seq integer(11) ,
  is_proxy integer(11) ,
  probe_id VARCHAR(32),
  area_name VARCHAR(64) ,
  longitude VARCHAR(12) ,
  latitude VARCHAR(12) ,
  group_name VARCHAR(64) ,
  group_id VARCHAR(32) ,
  showStatus integer(11) ,
  deep integer(11) ,
  CONSTRAINT PK PRIMARY KEY (flow_id),
  KEY idx_res_link_rel_node_ip (node_ip),
  KEY idx_res_link_rel_prb (probe_id)
);
