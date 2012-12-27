<?php
require('common.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');
$views = get_my_views(sess_get_uid());
$views2 = db_query_for_assoc("SELECT id, name FROM virtual_view WHERE user_id=? AND enabled>0", array(sess_get_uid()));
$groups = array();
foreach ($views as $id => $view) {
	$k = preg_replace('/\s+.*/','',$view);
	if (!$groups[$k])
		$groups[$k] = array();
	$groups[$k][] = $id;
}

$user_name=db_query_for_one('SELECT name FROM user WHERE id=?',array(sess_get_uid()));
if (ends_with($user_name,'@mozat.com') or is_admin()) {
	$v_view_tab = 'Personalized';
}
?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<link href="common.css" rel="stylesheet" type="text/css"/>
<style type="text/css">
	ol { font-size: 1.10em; line-height: 1.75em; padding-left:0; margin-left: 2em; }
	.right_link { margin-top: 5px; float:right;}

	input.groovybutton
	{
	   font-size:12px;
	   font-family:Arial,sans-serif;
	   font-weight:bold;
	   color:#444444;
	   background-color:#EEEEEE;
	   border-style:double;
	   border-color:#999999;
	   border-width:3px;
	}


</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" language="javascript" src="js/jquery.cookie.js"></script>
<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
		$('#tabs').tabs({cookie: { expires: 30, name: "ui-tabs-uv" }});
		if ($('#tabs').tabs('option','selected') < 0) {
			$('#tabs').tabs('option','selected', 0);
		}
	});
</script>
</head>
<body>
<div style="float:right; margin-right:3px;">

<?if(ends_with($user_name,'@mozat.com')){?>

<button type="button" class="img_btn" onclick="location.href='home.php'"><img src="images/home.png" height="24" /> Home</button>

<?}?>

<button type="button" class="img_btn" onclick="location.href='logout.php'"><img src="images/logout.png" height="24"/> Logout</button>&nbsp;

</div>
<h3>My Views</h3>
<div id="tabs">
<ul>
<?if ($v_view_tab) {?>
	<li><a href="#tabs-vv"><span><?=q($v_view_tab)?></span></a></li>
<?}?>
</ul>

<?if ($v_view_tab) {?>
<div id="tabs-vv">
<ol>
<?foreach ($views2 as $id=>$name) {?>
<li><a href="view_v.php?id=<?=$id?>"><?=$name?></a></li>
<?}?>
</ol>
<div align="right"><a href="edit_tab.php" class="img_link"><img src="images/setting.png" width="16" border="0"/><span>Create / Modify</span></a></div>
</div>
<?}?>
</div>

<?if(is_admin()){?>
<div class="right_link"><a href="user_management.php" target="_blank" style="color:gray;text-decoration:none;font-size:80%;">user management</a>
<?}?>

<p></p>
</body>
</html>