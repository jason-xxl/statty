<?php 
function connect_db($suffix='') {
	global $_CONFIG,$_PAGE;
    $dsn_opt = $_CONFIG["database$suffix"];
    $dsn_str = sprintf('%s:%sdbname=%s',
    	$dsn_opt['phptype'],
    	$dsn_opt['hostspec'] ? 'host='.str_replace(':',';port=',$dsn_opt['hostspec']).';' : '',
    	$dsn_opt['database']
    );
    $options = array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION);
    if ($dsn_opt['phptype'] == 'mysql' && $dsn_opt['persistent'])
        $options[PDO::ATTR_PERSISTENT] = TRUE;
    $_PAGE['db_conn'] = new PDO($dsn_str, $dsn_opt['username'], $dsn_opt['password'], $options);
    $_PAGE['db_conn']->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    if ($dsn_opt['initsql']) {
    	db_query($dsn_opt['initsql']);
    }
}
function db_conn() {
	return $GLOBALS['_PAGE']['db_conn'];
}
function db_update($sql, $params = NULL) {
	if (is_null($params))
		return db_conn()->exec($sql);
	$sth = db_conn()->prepare($sql);
	$sth->execute($params);
	return $sth->rowCount();
}
function db_query($sql, $params = NULL) {
	$sth = $params ? db_conn()->prepare($sql) : db_conn()->query($sql);
	if ($params) {
		foreach (array_keys($params) as $k) {
			if (is_string($k) && strpos($sql, ":$k") === FALSE)
				unset($params[$k]);
		}
		$sth = db_conn()->prepare($sql);
		$sth->execute($params);
	} else {
		$sth = db_conn()->query($sql);
	}
	return $sth;
}
function db_last_insert_id() {
	return db_conn()->lastInsertId();
}
function db_query_for_assoc($sql, $params = array()) {
	return array_map('reset',db_query($sql, $params)->fetchAll(PDO::FETCH_COLUMN|PDO::FETCH_GROUP));
}
function db_query_for_col($sql, $params = array()) {
	return db_query($sql, $params)->fetchAll(PDO::FETCH_COLUMN);
}
function db_query_for_one($sql, $params = NULL) {
	return db_query($sql, $params)->fetchColumn();
}
function db_query_for_json($sql, $params = NULL) {
	$j = db_query_for_one($sql, $params);
	return $j ? json_decode($j, TRUE) : array();
}
function db_fetch($sth, $fetch_style = PDO::FETCH_BOTH) {
	return $sth->fetch($fetch_style);
}
function db_fetch_array($sth) {
	return $sth->fetch(PDO::FETCH_NUM);
}
function db_fetch_assoc($sth) {
	return $sth->fetch(PDO::FETCH_ASSOC);
}
?>