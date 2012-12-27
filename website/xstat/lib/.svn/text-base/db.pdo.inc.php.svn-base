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
function connect_db_conn($suffix='', $conn_string='') {
	global $_CONFIG,$_PAGE;
	//echo $conn_string;
    $dsn_opt = json_decode($conn_string,True);
	//print_r($dsn_opt);exit();

	//{"phptype":"sqlsvr","username"="sa","password"="m0z@tm0r@nge","hostspec"="192.168.1.83:1533","database"="billing_umniah","persistent"="true","initsql"=""}

    $dsn_str = sprintf('%s:%s'.($dsn_opt['phptype']=='mysql'?'dbname=':'Database=').'%s',
    	$dsn_opt['phptype'],
    	$dsn_opt['hostspec'] ? ($dsn_opt['phptype']=='mysql'?'host=':'server=').str_replace(':',';port=',$dsn_opt['hostspec']).';' : '',
    	$dsn_opt['database']
    );
	//print $dsn_str;exit();
    $options = array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION);
    if ($dsn_opt['phptype'] == 'mysql' && $dsn_opt['persistent'])
        $options[PDO::ATTR_PERSISTENT] = TRUE;
    $_PAGE['db_conn'.$conn_string] = new PDO($dsn_str, $dsn_opt['username'], $dsn_opt['password'], $options);
    $_PAGE['db_conn'.$conn_string]->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    if ($dsn_opt['initsql']) {
    	db_query($dsn_opt['initsql'],$conn_string);
    }
}
function db_conn($conn_string='') {
	global $_CONFIG,$_PAGE;
	if(!empty($conn_string)){
		if(empty($_PAGE['db_conn'.$conn_string])){
			connect_db_conn('', $conn_string);
		}
		return $_PAGE['db_conn'.$conn_string];
	}
	return $GLOBALS['_PAGE']['db_conn'];
}
function db_update($sql, $params = NULL, $conn_string='') {
	if (is_null($params))
		return db_conn($conn_string)->exec($sql);
	$sth = db_conn($conn_string)->prepare($sql);
	$sth->execute($params);
	return $sth->rowCount();
}
function db_query($sql, $params = NULL, $conn_string='') {
	$sth = $params ? db_conn($conn_string)->prepare($sql) : db_conn($conn_string)->query($sql);
	if ($params) {
		foreach (array_keys($params) as $k) {
			//if (is_string($k) && strpos($sql, ":$k") === FALSE)
			if (is_string($k) && preg_match("/:$k\b/", $sql) === 0) //cos :k100 my overwrite :k10
				unset($params[$k]);
		}

		if (strpos($conn_string,'sqlsrv')!==False){
			$p=array();
			foreach($params as $k=>$v){
				$p[':'.$k]=$v;
			}
			$params=$p;
		}
		$sth = db_conn($conn_string)->prepare($sql);
		$sth->execute($params);
	} else {
		$sth = db_conn($conn_string)->query($sql);
	}
	return $sth;
}
function db_last_insert_id($conn_string='') {
	return db_conn($conn_string)->lastInsertId();
}
function db_query_for_assoc($sql, $params = array(), $conn_string='') {
	return array_map('reset',db_query($sql, $params,$conn_string)->fetchAll(PDO::FETCH_COLUMN|PDO::FETCH_GROUP));
}
function db_query_for_col($sql, $params = array(), $conn_string='') {
	return db_query($sql, $params,$conn_string)->fetchAll(PDO::FETCH_COLUMN);
}
function db_query_for_one($sql, $params = NULL, $conn_string='') {
	return db_query($sql, $params,$conn_string)->fetchColumn();
}
function db_query_for_json($sql, $params = NULL, $conn_string='') {
	$j = db_query_for_one($sql, $params,$conn_string);
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