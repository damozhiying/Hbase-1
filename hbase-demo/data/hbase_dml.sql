/*模拟查询条件*/
SELECT count(1) FROM (
SELECT t4.cdnIp,t4.avgRespDelay FROM (
SELECT AVG(TO_NUMBER(a.index_val)) AS avgRespDelay ,a.ext_col AS cdnIp
FROM "C_CDN_INDEX" a
WHERE a.start_time >= TO_TIMESTAMP('2017-10-23 08:00:00.0') AND a.start_time < TO_TIMESTAMP('2017-10-24 08:00:00.0') AND a.index_id IN('spDelay','vodDelay','tvodDelay','tstvDelay') AND a.index_val > '1'
GROUP BY a.ext_col) t4 ) t
INNER JOIN (SELECT node_name,node_ip FROM res_link_relation WHERE busi_type = 1)t2 ON t.cdnIp = t2.node_ip
/*******************模拟查询条件2********************************
1、该条件下，现网下查询1800万条数据需要20多分钟
2、现网上每个小时产生1800万条数据
*/
SELECT c.client_id,c.account_code,c.wlan_ip,t.* FROM (
		SELECT probe_id,index_id,AVG(to_number(index_val)) avg_val,SUM(to_number(index_val)) sum_val
		FROM c_cdn_index
		WHERE index_id in ('cdn2XX','cdn4XX','cdn5XX','spDelay','vodDelay','tvodDelay','tstvDelay')
		AND deal_flag=1
		AND start_time>=TO_TIMESTAMP('2017-10-27 08:59:59')
		AND start_time<=TO_TIMESTAMP('2017-10-27 09:59:59')
		GROUP BY probe_id,index_id
) t INNER JOIN client_info c ON t.probe_id=c.probe_id;
-- phoenix可执行语句
SELECT c.client_id,c.account_code,c.wlan_ip,t.* FROM ( SELECT probe_id,index_id,AVG(to_number(index_val)) avg_val,SUM(to_number(index_val)) sum_val FROM c_ott_qoe WHERE index_id in ('cdn2XX','cdn4XX','cdn5XX','spDelay','vodDelay','tvodDelay','tstvDelay') AND deal_flag=1 AND start_time>=TO_TIMESTAMP('2017-10-27 08:59:59') AND start_time<=TO_TIMESTAMP('2017-10-27 09:59:59') GROUP BY probe_id,index_id ) t INNER JOIN client_info c ON t.probe_id=c.probe_id;
/*查询1小时的probe_id*/
SELECT probe_id FROM c_cdn_index WHERE start_time>=TO_TIMESTAMP('2017-10-27 08:59:59') AND start_time<=TO_TIMESTAMP('2017-10-27 09:59:59');
/*****************************MySQL和Phoenix关联查询时间对比************************************/
-- phoenix查询SQL
SELECT c.client_id,c.account_code,c.wlan_ip,t.* FROM (SELECT probe_id,index_id,AVG(to_number(index_val)) avg_val,SUM(to_number(index_val)) sum_val FROM c_ott_qoe WHERE index_id IN ('mos','pauseLast','pauseTimes') AND deal_flag=1 AND start_time>='2017-10-23 23:59:59' AND start_time<='2017-10-23 24:59:59' GROUP BY probe_id,index_id) t INNER JOIN client_info c ON t.probe_id=c.probe_id;
-- MySQL查询SQL
select c.client_id,c.account_code,c.wlan_ip,t.* from (
		select probe_id,index_id,avg(index_val) avg_val,sum(index_val) sum_val
		from c_ott_qoe where index_id in ('mos','pauseLast','pauseTimes')
		and deal_flag=1
		and start_time>= '2017-10-23 23:59:59'
		and start_time<='2017-10-23 24:59:59'
		group by probe_id,index_id
) t inner join client_info c on t.probe_id=c.probe_id
/*******************************res_link_relation插入数据语句*********************************/
upsert INTO res_link_relation VALUES (9, 'epg_1.1.1.1', null, '-1', '123.147.112.250', 2, null, null, 'epg1', null, null, null, 'epg', 'cfe519eb69d040f2b0ffbe4520e2faa6', 1, 2);
upsert INTO res_link_relation VALUES (10, 'epg_2.2.2.2', null, '-1', '123.147.112.250', 2, null, null, 'epg1', null, null, null, 'epg', 'cfe519eb69d040f2b0ffbe4520e2faa6', 1, 2);
upsert INTO res_link_relation VALUES (14, 'EPG3_123.147.112.72', null, '-1', '123.147.112.250', 2, null, null, null, '丰都县', '107.73730947', '29.8695925', 'EPG3', '741b404c80a6409db787911331309937', 1, 1);
upsert INTO res_link_relation VALUES (15, 'EPG3_123.147.112.73', null, '-1', '123.147.112.250', 2, null, null, null, '涪陵区', '107.396241', '29.709467', 'EPG3', '741b404c80a6409db787911331309937', 1, 1);
upsert INTO res_link_relation VALUES (19, 'j1_12.12.12.11', null, '-1', '123.147.112.250', 2, null, null, '100098', null, null, null, 'j1', 'c3660142ca614a4a947127ad7a7f2f2b', 1, 4);
upsert INTO res_link_relation VALUES (128, 'cdn区域ffffffff_123.147.112.29', null, '2', '123.147.112.251', 1, null, null, '21000001', null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (129, 'cdn区域ffffffff_106.57.175.3', null, '2', '123.147.112.251', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (130, 'cdn区域ffffffff_106.57.175.10', null, '2', '123.147.112.251', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (131, 'cdn区域ffffffff_106.57.175.4', null, '2', '123.147.112.251', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (132, 'cdn区域ffffffff_106.57.175.7', null, '2', '123.147.112.251', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (133, 'cdn区域ffffffff_106.57.175.9', null, '2', '123.147.112.25', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (134, 'cdn区域ffffffff_222.220.72.2', null, '2', '123.147.112.25', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (135, 'cdn区域ffffffff_222.220.72.18', null, '2', '123.147.112.25', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (136, 'cdn区域ffffffff_222.220.72.34', null, '2', '123.147.112.25', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (137, 'cdn区域ffffffff_222.220.72.50', null, '2', '123.147.112.26', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (138, 'cdn区域ffffffff_222.220.72.66', null, '2', '123.147.112.26', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (139, 'cdn区域ffffffff_222.220.72.82', null, '2', '123.147.112.26', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (140, 'cdn区域ffffffff_222.220.72.98', null, '2', '123.147.112.26', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (141, 'cdn区域ffffffff_222.220.72.114', null, '2', '123.147.112.26', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (142, 'cdn区域ffffffff_222.220.72.130', null, '2', '123.147.112.27', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (143, 'cdn区域ffffffff_222.220.72.146', null, '2', '123.147.112.27', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (144, 'cdn区域ffffffff_222.220.72.162', null, '2', '123.147.112.27', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (145, 'cdn区域ffffffff_222.220.72.3', null, '2', '123.147.112.28', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (146, 'cdn区域ffffffff_222.220.72.19', null, '2', '123.147.112.28', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (147, 'cdn区域ffffffff_222.220.72.35', null, '2', '123.147.112.28', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (148, 'cdn区域ffffffff_222.220.72.51', null, '2', '123.147.112.28', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (149, 'cdn区域ffffffff_222.220.72.67', null, '2', '123.147.112.29', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (150, 'cdn区域ffffffff_222.220.72.83', null, '2', '123.147.112.29', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (151, 'cdn区域ffffffff_222.220.72.99', null, '2', '123.147.112.29', 1, null, null, null, null, null, null, 'cdn区域ffffffff', 'cecc981877294c0795e65a2c4a01c101', 1, 3);
upsert INTO res_link_relation VALUES (153, 'cdn中_123.147.113.133', null, '1', '123.147.112.30', 1, null, null, null, null, null, null, 'cdn中', '0a85a8313e7c45e698d2cc811347c8a9', 1, 2);
upsert INTO res_link_relation VALUES (154, 'cdn中_123.147.113.134', null, '1', '123.147.112.30', 1, null, null, null, null, null, null, 'cdn中', '0a85a8313e7c45e698d2cc811347c8a9', 1, 2);
upsert INTO res_link_relation VALUES (156, 'sdfsdfd_119.6.241.115', null, '-1', '123.147.112.30', 2, null, null, null, null, null, null, 'sdfsdfd', '0251833e30524352a5980c4310b96d3b', 1, 4);
upsert INTO res_link_relation VALUES (157, 'cdn中_101.207.178.10', null, '1', '123.147.112.31', 1, null, null, null, null, null, null, 'cdn中', '0a85a8313e7c45e698d2cc811347c8a9', 1, 2);
upsert INTO res_link_relation VALUES (159, 'cdn区域02_123.147.112.25', null, '2', '123.147.112.31', 1, null, null, null, null, null, null, 'cdn区域02', 'db9956bf93884b71acfc4d1b13a6c7c6', 1, 3);
upsert INTO res_link_relation VALUES (160, 'cdn区域02_123.147.112.29', null, '2', '123.147.112.31', 1, null, null, null, null, null, null, 'cdn区域02', 'db9956bf93884b71acfc4d1b13a6c7c6', 1, 3);