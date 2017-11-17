DROP TABLE IF EXISTS c_cdn_index;
CREATE TABLE c_cdn_index (
  flow_id bigint(20) NOT NULL AUTO_INCREMENT COMMENT '流水号',
  collect_id int(11) DEFAULT NULL COMMENT '采集记录ID',
  client_id varchar(64) DEFAULT NULL COMMENT '终端ID',
  account varchar(64) DEFAULT NULL,
  stb_ip varchar(32) DEFAULT NULL COMMENT '机顶盒IP',
  probe_id varchar(10) DEFAULT NULL COMMENT '探针ID',
  index_id varchar(16) DEFAULT NULL COMMENT '指标ID',
  index_val varchar(64) DEFAULT NULL COMMENT '指标值',
  start_time datetime NOT NULL COMMENT '采样开始时间',
  end_time datetime DEFAULT NULL COMMENT '采样结束时间',
  up_time datetime DEFAULT NULL COMMENT '上报时间',
  deal_flag int(11) DEFAULT NULL COMMENT '播放状态,1-正在播放,2-未播放',
  ext_col varchar(64) DEFAULT NULL,
  service_type int(11) DEFAULT NULL,
  PRIMARY KEY (flow_id,start_time),
  KEY idx_c_cdn_prb (probe_id) USING BTREE,
  KEY idx_c_cdn_idx (index_id) USING BTREE,
  KEY idx_c_cdn_id_val_start (index_id,index_val,start_time) USING BTREE,
  KEY idx_c_cdn_idx_val (index_val) USING BTREE,
  KEY idx_c_cdn_ext_col (ext_col) USING BTREE
)