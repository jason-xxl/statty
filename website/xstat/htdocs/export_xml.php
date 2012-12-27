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
	foreach($view['cols'] as $a1=>$a2){
		$view['cols'][$a1]=strip_tags($a2);
	}
	$values[] = array_values($row);
	while ($row = db_fetch_array($sth)) {
		$values[] = $row;
	}
}
set_download_header("text/xml", date('Ymd').".xml");
//echo '#', $view['name'], $lf;
echo '<?xml version="1.0" encoding="UTF-8" ?>';
echo '<root>';
//foreach ($view['cols'] as $j => $k) {
//}

if ($_GET['unfold_collection']==1){
	foreach ($values as $i => $row) {
		if(!empty($row[0])){
			$elements=explode(',',$row[0]);
			foreach($elements as $j => $v){
				if ($j == 0)
					printf('<row name="%s">', htmlentities($v));
				printf('<col name="%s">%s</col>', htmlentities($view['cols'][$j]), 
					htmlentities(preg_replace('/<[^>]*>/', '', $v)));
			}
			echo '</row>';
		}
	}
}else{
	foreach ($values as $i => $row) {
		foreach ($row as $j => $v) {
			if ($j == 0)
				printf('<row id="%s">', htmlentities($v));
			printf('<col name="%s">%s</col>', htmlentities($view['cols'][$j]), 
				htmlentities(preg_replace('/<[^>]*>/', '', $v)));
		}
		echo '</row>';
	}
}
echo '</root>';
?>