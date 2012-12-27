<?php
set_time_limit(180);
require('common.inc.php');
page_start();
require('xstat.inc.php');
if (!is_admin())
	die('access_denied');

$params=array();

if (!is_post()) {

} elseif ($_POST['action'] == 'add') {
	foreach ($_POST['user'] as $user_id)
		$c = db_query('INSERT IGNORE INTO user_to_group (group_id,user_id) VALUES (?,?)',array($_GET['id'],$user_id));
	redirect($_PAGE['.']);
} elseif ($_POST['action']=='del') {
	foreach ($_POST['user'] as $user_id)
		$c = db_query('DELETE FROM user WHERE id=?',array($user_id));
	redirect($_PAGE['.']);
} elseif ($_POST['action'] == 'add_user') {
	$c = db_query('INSERT IGNORE INTO `user` (name,pw) VALUES (?,MD5(?))',array($_POST['name'],$_POST['passwd']));
	redirect($_PAGE['.']);
}

$users = db_query_for_assoc("SELECT id,name FROM `user` order by name asc",$params);

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

<b>User Accounts</b>

<div class="rc_box">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="del"/>
<table>
<?foreach ($users as $uid => $name) {?>
<tr>
<td>
<?if ($name!='admin') {?>
<input type="checkbox" name="user[]" value="<?=$uid?>"/>
<?}?>
</td>
<td style="font-size:12px;"><?=$name?></td>
<td style="color:gray; font-size:12px;">
<?
$groups = db_query_for_assoc("SELECT `user_to_group`.group_id,`group`.name FROM `user_to_group` left join `group` on `user_to_group`.group_id=`group`.id where `user_to_group`.user_id=? order by `group`.name",array($uid));
foreach ($groups as $gid => $gname){
	?><a href="group_user.php?id=<?=$gid?>"><?=$gname?></a> <?
}
?>
<a href="user_view.php?d_uid=<?=$uid?>" target="_blank">(Accesible Views)</a>
<a href="view.php?id=978&key1=<?=$uid?>&key2=1" target="_blank">(View History)</a>
</td>
</tr>
<?}?>

<?if (!$uid) {?>
<tr><td><font color="blue"> No user account now. </font></td></tr>
<?}elseif (array_values($users) != array('admin')) {?>
<tr><td colspan="2"><input onclick="if(!confirm('Sure to delete?')){return false;}else{return true;}" type="submit" value="Permenantly Remove Selected User Accounts"/></td></tr>
<?}?>
</table>
</form>
</div>

<p></p>

<b>Create User Account</b>

<div class="rc_box">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="add_user"/>
Name: <input type="text" name="name"/>
Password: <input type="text" name="passwd"/>
<input type="submit" value="Create"/>
</form>
</div>


</body>
</html>