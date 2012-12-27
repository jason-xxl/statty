<?php
require('common.inc.php');
require('sql_preprocess.inc.php');

set_time_limit(720);
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');
$_PAGE['view_id'] = $_GET['id'];
if (!test_access($_PAGE['view_id']))
	die('access denied');
$view = db_fetch_assoc(db_query('SELECT name,`sql`,chart,day_range_default,conn_string FROM view WHERE view.id=?',array($_PAGE['view_id'])));
$params = get_default_params();
$values = $view['cols'] = array();
$sql = $_GET['group_by']?getAggregatingSql($view['sql'],$params,$_GET['group_by'],$_GET['id']):$view['sql'];
$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $sql);
$sql = str_replace('%where_and_more_compact%', ' AND `date`>=convert(replace(:date1,\'-\',\'\'),DECIMAL) AND `date`<=convert(replace(:date2,\'-\',\'\'),DECIMAL)', $sql);
$sql=replace_get_md5_key($sql);
$sql=replace_get_md5_sub_key($sql);

$sth = db_query($sql, $params,$view['conn_string']);
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

require('Spreadsheet/Excel/Writer.php');
$workbook = new Spreadsheet_Excel_Writer();
$format_bold =& $workbook->addFormat();
$format_bold->setBold();

$worksheet =& $workbook->addWorksheet();
$worksheet->write(0, 0, $view['name'], $format_bold);
foreach ($view['cols'] as $j => $k) {
	$worksheet->write(1, $j, $k, $format_bold);
}

if ($_GET['unfold_collection']==1){
	foreach ($values as $i => $row) {
		if(!empty($row[0])){
			$elements=explode(',',$row[0]);
			//var_dump($elements);die();

			foreach($elements as $j => $e){
				$worksheet->writeString($j+2, 0, preg_replace('/<[^>]+>/', '', $e));
			}
		}
		//break;
	}
}else{
	foreach ($values as $i => $row) {
		foreach ($row as $j => $v) {
			$worksheet->write($i+2, $j, trim(preg_replace('/<[^>]+>/', '', $v)));
		}
	}
}

$workbook->send(date('Ymd').".xls");
$workbook->close();
?>