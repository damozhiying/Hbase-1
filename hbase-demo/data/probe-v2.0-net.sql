/*mysql查询的SQL*/
SELECT
	count(1)
FROM
	(
		SELECT
			t4.cdnIp,
			t4.startTime,
			t4.avgRespDelay
		FROM
			(
				SELECT
					from_unixtime(
						floor(
							unix_timestamp(a.start_time) / 60 / 5
						) * 60 * 5,
						'%Y-%m-%d %H:%i'
					) AS startTime,
					AVG(a.index_val) AS avgRespDelay,
					a.ext_col AS cdnIp
				FROM
					c_cdn_index a
				WHERE
					a.start_time >= '2017-10-22'
				AND a.start_time < '2017-10-23'
				AND FIND_IN_SET(
					a.index_id,
					'spDelay,vodDelay,tvodDelay,tstvDelay'
				)
				AND a.index_val > 1000.0
				GROUP BY
					a.ext_col,
					from_unixtime(
						floor(
							unix_timestamp(a.start_time) / 60 / 5
						) * 60 * 5
					)
			) t4
	) t
INNER JOIN (
	SELECT
		node_name,
		node_ip
	FROM
		res_link_relation
	WHERE
		busi_type = '1'
) t2 ON t.cdnIp = t2.node_ip

/*hbase的查询SQL*/
SELECT
	count(1)
FROM
	(
		SELECT
			t4.cdnIp,
			t4.startTime,
			t4.avgRespDelay
		FROM
			(
				SELECT
					TO_DATE(
						floor(
							TO_TIMESTAMP(TO_CHAR(a.start_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 5
						) * 60 * 5,
						'%Y-%m-%d %H:%i'
					) AS startTime,
					AVG(TO_NUMBER(a.index_val)) AS avgRespDelay ,
					a.ext_col AS cdnIp
				FROM
					c_cdn_index a
				WHERE
					a.start_time >= '2017-10-22'
				AND a.start_time < '2017-10-23'
				AND a.index_id IN(
					'spDelay,vodDelay,tvodDelay,tstvDelay'
				)
				AND a.index_val > 1000.0
				GROUP BY
					a.ext_col,
					TO_DATE(
						floor(
							TO_TIMESTAMP(TO_CHAR(a.start_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 5
						) * 60 * 5
					)
			) t4
	) t
INNER JOIN (
	SELECT
		node_name,
		node_ip
	FROM
		res_link_relation
	WHERE
		busi_type = '1'
) t2 ON t.cdnIp = t2.node_ip

/*hbase函数测试*/
select TO_TIMESTAMP(TO_CHAR(a.start_time,'yyyy-MM-dd HH:mm:ss')) from c_cdn_index a
/*模拟查询条件*/
SELECT count(1) FROM (
SELECT t4.cdnIp,t4.avgRespDelay FROM (
SELECT AVG(TO_NUMBER(a.index_val)) AS avgRespDelay ,a.ext_col AS cdnIp
FROM "C_CDN_INDEX" a
WHERE a.start_time >= TO_TIMESTAMP('2017-10-23 08:00:00.0') AND a.start_time < TO_TIMESTAMP('2017-10-24 08:00:00.0') AND a.index_id IN('spDelay','vodDelay','tvodDelay','tstvDelay') AND a.index_val > '1'
GROUP BY a.ext_col) t4 ) t
INNER JOIN (SELECT node_name,node_ip FROM res_link_relation WHERE busi_type = 1)t2 ON t.cdnIp = t2.node_ip
