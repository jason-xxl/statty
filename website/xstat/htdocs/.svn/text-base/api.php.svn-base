<?php
set_time_limit(180);
require('common.inc.php');
require('xstat.inc.php');

connect_db();

$user_id = db_query_for_one('SELECT id FROM user WHERE name=? AND pw=MD5(?)',array($_GET['user'],$_GET['pass']));
if (empty($user_id)){

	die('access denied 1.');
}

if (!empty($user_id)){
	$views = get_my_views($user_id, TRUE);
	if (empty($views[$_GET['id']])){
		die('access denied 2.');
	}
}

//$view = db_fetch_assoc(db_query('SELECT name,`sql`,chart FROM view JOIN user_view ON view_id=view.id WHERE user_id=? AND view.id=?',array($user_id,$_GET['id'])));
$view = db_fetch_assoc(db_query('SELECT name,`sql`,chart FROM view WHERE id=?',array($_GET['id'])));
$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $view['sql']);
$values = $keys = $charts = array();
$sth = db_query($sql, $_GET);
if ($row = db_fetch_assoc($sth)) {
	$keys = array_keys($row);
	$values[] = array_values($row);
	while ($row = db_fetch_array($sth)) {
		$values[] = $row;
	}
}

header('Content-Type: application/javascript');
echo 'var stat_keys = '.json_encode($keys).";\n";
echo 'var stat_vals = '.json_encode($values).";\n";

?>