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
	foreach ($_POST['user'] as $user_id)
		$c = db_query('INSERT IGNORE INTO user_to_group (group_id,user_id) VALUES (?,?)',array($_GET['id'],$user_id));
	redirect($_PAGE['.']);
} elseif ($_POST['action']=='del') {
	foreach ($_POST['user'] as $user_id)
		$c = db_query('DELETE FROM user_to_group WHERE group_id=? AND user_id=? AND id>1',array($_GET['id'],$user_id));
	redirect($_PAGE['.']);
}
$group = db_fetch_assoc(db_query('SELECT * FROM `group` WHERE id=?',$params));
$users = db_query_for_assoc("SELECT id,name FROM `user` WHERE id NOT IN (SELECT user_id FROM user_to_group WHERE group_id=?) AND name<>'admin'",$params);
$group_users = db_query_for_assoc('SELECT user_id,user.name FROM `user_to_group` JOIN `user` ON user_id = `user`.id WHERE group_id=?',$params);
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

<b>Group Users</b>

<p></p>
<div>Group Name: <?=q($group['name'])?></div>
<div>Description: <?=q($group['description'])?></div>
<p></p>

<div class="rc_box">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="del"/>
<table>
<?foreach ($group_users as $uid => $name) {?>
<tr>
<td>
<?if ($name!='admin') {?>
<input type="checkbox" name="user[]" value="<?=$uid?>"/>
<?}?>
</td>
<td style="font-size:12px;"><?=$name?></td>
</tr>
<?}?>
<?if (!$uid) {?>
<tr><td><font color="blue"> No user in the group now. </font></td></tr>
<?}elseif (array_values($group_users) != array('admin')) {?>
<tr><td colspan="2"><input type="submit" value="Remove From Group"/></td></tr>
<?}?>
</table>
</form>
</div>

<p></p>

<b>Add User</b>

<div class="rc_box">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="add"/>
<select multiple="multiple" name="user[]">
<?foreach ($users as $id => $user) {?>
<option value="<?=$id?>"><?=q($user)?></option>
<?}?>
</select>
<input type="submit" value="Add Into Group"/>
<font style="color:gray; font-size:10px;">Keep pressing CTRL to multi-select.</font>
</form>
</div>

<p></p>

<button onclick="history.back();">&lt;&lt; Back</button>

</body>
</html>