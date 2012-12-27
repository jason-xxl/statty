<?php
set_time_limit(180);
require('common.inc.php');
page_start();
require('xstat.inc.php');
if (!is_admin())
	die('access_denied');
$params = array($_GET['id']);
if (!is_post()) {
} elseif ($_POST['action'] == 'add') {
	foreach ($_POST['view'] as $view_id)
		$c = db_query('INSERT IGNORE INTO group_to_view (group_id,view_id) VALUES (?,?)',array($_GET['id'],$view_id));
	redirect($_PAGE['.']);
} elseif ($_POST['action']=='del') {
	foreach ($_POST['view'] as $view_id)
		$c = db_query('DELETE FROM group_to_view WHERE group_id=? AND view_id=?',array($_GET['id'],$view_id));
	redirect($_PAGE['.']);
}
$group = db_fetch_assoc(db_query('SELECT * FROM `group` WHERE id=?',$params));
$views = db_query_for_assoc('SELECT id,name FROM `view` WHERE id NOT IN (SELECT view_id FROM group_to_view WHERE group_id=?) order by name asc',$params);
$group_views = db_query_for_assoc('SELECT view_id,view.name FROM `group_to_view` JOIN `view` ON view_id = `view`.id WHERE group_id=? order by view.name asc',$params);
?>
<html>
<head>
<style type="text/css">
	.rc_box { border: 1px solid gray; margin:8px; padding:3px; font-size:12px;}
	form { margin:2px; padding:2px;}
</style>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" src="./js/jquery.curvycorners.packed.js"></script>

<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
		$('.rc_box').corner();
	});
</script>
</head>
<body>
<b>Accesible Views For <?=q($group['name'])?> Group</b>
<p></p>
<div>Group Description: <?=q($group['description'])?></div>
<p></p>



<b>Inaccessible Views</b>

<div class="rc_box">
<table>
<tr valign="top"><td>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="add"/>
<select multiple="multiple" name="view[]" style="height: 300px">
<?foreach ($views as $id => $view) {?>
<option value="<?=$id?>"><?=q($view)?></option>
<?}?>
</select>
<input type="submit" value="Add View To Group"/>
<font style="color:gray; font-size:10px;">Keep pressing CTRL to multi-select.</font>

</form>
</td></tr>
</table>
</div>



<p></p>

<b>Accessible Views</b>

<div class="rc_box">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="del"/>
<table>
<?foreach ($group_views as $vid => $name) {?>
<tr>
<td>
<input type="checkbox" name="view[]" value="<?=$vid?>"/>
</td>
<td style="font-size:12px;"><?=$name?> <a href="view.php?id=<?=$vid?>" target="_blank">(open)</a></td>
</tr>
<?}?>
<?if (!$vid) {?>
<tr><td><font color="blue"> No accessible views for the group. </font></td></tr>
<?}else {?>
<tr><td colspan="2"><input type="submit" value="Remove From Group"/></td></tr>
<?}?>
</table>
</form>
</div>
<p></p>
<button onclick="history.back();">&lt;&lt; Back</button>
</body>
</html>