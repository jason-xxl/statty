<?php
set_time_limit(180);
require('common.inc.php');
page_start();
require('xstat.inc.php');
if (!is_admin())
	die('access_denied');
if (!is_post()) {
} elseif ($_POST['action'] == 'add_group') {
	$c = db_query('INSERT IGNORE INTO `group` (name,description) VALUES (?,?)',array($_POST['name'],$_POST['descr']));
	redirect($_PAGE['.']);
} elseif ($_POST['action'] == 'add_user') {
	$c = db_query('INSERT IGNORE INTO `user` (name,pw) VALUES (?,MD5(?))',array($_POST['name'],$_POST['passwd']));
	redirect($_PAGE['.']);
}
$sth = db_query('SELECT * FROM `group` order by name');
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
<b>Groups</b>
<p></p>

<div class="rc_box">
<table>
<?while ($row = db_fetch_assoc($sth)) {?>
<tr style="font-size:12px;">
<td><span title="<?=q($row['description'])?>"><?=q($row['name'])?></span></td>
<td><a href="group_user.php?id=<?=$row['id']?>">Users</a></td>
<td>
<?if ($row['name']!='admin') {?>
<a href="group_view.php?id=<?=$row['id']?>">Views</a>
<?}else{
?>(All Views)<?
}?>
</td>
</tr>
<?}?>
</table>
</div>

<p></p>

<b>Create Group</b>

<div class="rc_box">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="action" value="add_group"/>
Name: <input type="text" name="name"/>
Description: <input type="text" name="descr"/>
<input type="submit" value="Create"/>
</form>
</div>

</body>
</html>