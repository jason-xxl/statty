<?php
require('common.inc.php');
page_start();

if (!$_SESSION[$_PAGE['.']])
	$_SESSION[$_PAGE['.']] = array();
$_P =& $_SESSION[$_PAGE['.']];
if (is_post()) {
	$_P['date1'] = trim($_POST['date1']);
	$_P['date2'] = trim($_POST['date2']);
} elseif (!$_P) {
	$_P['date1'] = date('Y-m-d',strtotime('-1 month'));
	$_P['date2'] = date('Y-m-d');
}
$ks = db_fetch_assoc(db_query('SELECT oem_name,category,`key` FROM raw_data WHERE id = ?', array($_GET['id'])));
$sql = 'SELECT date, sub_key, value FROM raw_data WHERE oem_name=? AND category=? and `key`=? AND date BETWEEN ? AND ? ORDER BY date DESC';
$values = $keys = array();
$sth = db_query($sql, array($ks['oem_name'],$ks['category'],$ks['key'],$_P['date1'],$_P['date2']));
while ($row = db_fetch_assoc($sth)) {
	if (!($i = $keys[$row['sub_key']]))
		$i = $keys[$row['sub_key']] = count($keys);
	$values[$row['date']][$i] = $row['value'];
}
?>
<html>
<head>
<style type="text/css">
	div.flot-graph { margin:20px 0; }
	thead td {text-align:center; vertical-align:bottom; padding:3px 3px 3px 3px;font-weight: bold;color:#666666; }
	tbody td {text-align:right; padding:3px 3px 3px 3px }
</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
	} );
</script>

</head>
<body>
<h3><?=q($ks['oem_name'].' | '.$ks['category'].' | '.$ks['key'])?></h3>
<form action="<?=$_PAGE['.']?>" method="post">
<div>Date between: <input type="text" name="date1" value="<?=q($_P['date1'])?>"/> <input type="text" name="date2" value="<?=q($_P['date2'])?>"/>
<input class="groovybutton" type="submit" value="Reload"/>
</div></form>
<table border="1" class="main_table">
<thead>
<tr>
<td>Date/Time</td>
<?foreach ($keys as $k => $v) {?>
<td><?=$k?></td>
<?}?>
</tr>
</thead>

<tbody>
<?foreach ($values as $d => $row) {?>
<tr>
<td><?=$d?>&nbsp;</td>
<?foreach ($keys as $i) {?>
<td><?=preg_replace('/(\.\d\d)\d+$/', '\1', $row[$i])?>&nbsp;</td>
<?}?>
</tr>
<?}?>
</tbody>
</table>
<br/>
<div><button onclick="location.href='user_view.php'">Back</button></div>
</body>
</html>