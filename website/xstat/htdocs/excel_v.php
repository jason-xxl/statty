<?php
require('common.inc.php');
set_time_limit(720);
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');
$_PAGE['view_id'] = $_GET['id'];
$view = db_fetch_assoc(db_query('SELECT * FROM virtual_view WHERE id=? AND user_id=?',array($_PAGE['view_id'],sess_get_uid())));
if (!$view)
	die('access denied');
$view['opt'] = json_decode(if_blank($view['options'],'[]'), TRUE);
$views = get_my_views(sess_get_uid());
$params = get_default_params();
$values = $view['cols'] = $keys = $charts = array();
$sth = db_query('SELECT `sql`,view_id,col_name,alias FROM virtual_view_item, view WHERE view_id=view.id AND virtual_view_id=? ORDER BY seq',array($_PAGE['view_id']));
for ($i = 0; $row = db_fetch_assoc($sth); $i++) {
	if ($views[$row['view_id']])
		$keys[$row['sql']][$i] = $row['col_name'];
	$view['cols'][] = ors($row['alias'],$row['col_name']);
}
if (!$view['opt']['non_time_key'] && !$_GET['group_by']) {
	for ($i = strtotime($_GET['date2']); ($t=date('Y-m-d',$i)) >= $_GET['date1']; $i-=SECONDS_PER_DAY)
		$values[$t] = array($t);
}
foreach ($keys as $s => $names) {
	$sql = $_GET['group_by']?getAggregatingSql($s,$params,$_GET['group_by'],$_GET['id']):$s;
	$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $sql);
	$sth = db_query($sql, $params);
	for ($i = 0; $row = db_fetch_assoc($sth); $i++) {
		list($k,$t) = each($row);
		if ($_GET['group_by'])
			$values[$t][0] = $t;
		if ($names[0] && $i == 0)
			$view['key'] = $k;
		foreach ($names as $j => $name)
			$values[$t][$j+1] = $row[$name];
	}
}

require('Spreadsheet/Excel/Writer.php');
$workbook = new Spreadsheet_Excel_Writer();
$format_bold =& $workbook->addFormat();
$format_bold->setBold();

$worksheet =& $workbook->addWorksheet();
$worksheet->write(0, 0, $view['name'], $format_bold);
$worksheet->write(1, 0, $view['key'], $format_bold);
foreach ($view['cols'] as $j => $k) {
	$worksheet->write(1, $j+1, $k, $format_bold);
}
foreach (array_values($values) as $i => $row) {
	foreach (range(0, count($view['cols'])) as $j)
		$worksheet->write($i+2, $j, preg_replace('/<[^>]+>/', '', $row[$j]));
}
$workbook->send(date('Ymd').".xls");
$workbook->close();
?>