<?php
set_time_limit(180);
require('common.inc.php');
require('xstat.inc.php');

connect_db();

$user_id = db_query_for_one('SELECT id FROM user WHERE name=? AND pw=MD5(?)',array($_GET['user'],$_GET['pass']));
if (empty($user_id)){
	header('HTTP/1.1 401 Unauthorized');
	die('access denied 1.');
}

if (!empty($user_id)){
	$views = get_my_views($user_id, TRUE);
	if (!$_GET['id'])
		stop(json_encode($views));
	if (empty($views[$_GET['id']])){
		if ($_GET['id'] > max(array_keys($views)))
			header('HTTP/1.1 400 Bad Request');
		else
			header('HTTP/1.1 403 Forbidden');
		die('access denied 2.');
	}
}

$view = db_fetch_assoc(db_query('SELECT name,`sql`,chart,conn_string FROM view WHERE id=?',array($_GET['id'])));
$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $view['sql']);
$sql = str_replace('%where_and_more_compact%', ' AND `date` BETWEEN DATE(:date1)+0 AND DATE(:date2)+0', $sql);
$values = $keys = $charts = array();
$sth = db_query($sql, $_GET, $view['conn_string']);
if ($row = db_fetch_assoc($sth)) {
	$keys = array_keys($row);
	$values[] = array_values($row);
	while ($row = db_fetch_array($sth)) {
		$values[] = $row;
	}
}

header('Content-Type: application/javascript');
echo json_encode(array('keys'=>$keys,'values'=>$values,'name'=>$views[$_GET['id']]));
?>
