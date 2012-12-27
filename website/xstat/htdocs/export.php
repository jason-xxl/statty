<?php
$separator = $_GET['s'];
require('common.inc.php');
set_time_limit(720);
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');
$_PAGE['view_id'] = $_GET['id'];
if (!test_access($_PAGE['view_id']))
	die('access denied');
$view = db_fetch_assoc(db_query('SELECT name,`sql`,chart,day_range_default FROM view WHERE view.id=?',array($_PAGE['view_id'])));
$params = get_default_params();
$values = $view['cols'] = array();
$sql = $_GET['group_by']?getAggregatingSql($view['sql'],$params,$_GET['group_by'],$_GET['id']):$view['sql'];
$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $sql);
$sth = db_query($sql, $params);
if ($row = db_fetch_assoc($sth)) {
	$view['cols'] = array_keys($row);
	$values[] = array_values($row);
	while ($row = db_fetch_array($sth)) {
		$values[] = $row;
	}
}
set_download_header("text/plain", date('Ymd').".txt");
$quot = $_GET['q'];
$lf = "\r\n";
echo '#', $view['name'], $lf;
foreach ($view['cols'] as $j => $k) {
	if ($j > 0) echo $separator;
	echo $quot, strip_tags($k), $quot;
}
echo $lf;

if ($_GET['unfold_collection']==1){
	foreach ($values as $i => $row) {
		if(!empty($row[0])){
			$elements=explode(',',$row[0]);
			foreach($elements as $j => $e){
				if ($j > 0) echo $separator;
				echo $quot, preg_replace('/<[^>]*>/', '', $e), $quot;
			}
			echo $lf;
		}
	}
}else{
	foreach ($values as $i => $row) {
		foreach ($row as $j => $v) {
			if ($j > 0) echo $separator;
			echo $quot, trim(preg_replace('/<[^>]*>/', '', $v)), $quot;
		}
		echo $lf;
	}
}
?>