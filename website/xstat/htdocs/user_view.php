<?php
require('common.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');

$views_uid=sess_get_uid();

if (is_post() && $_POST['action'] == 'change_pw') {
	if(!empty($_POST['new_pw']) && !empty($_POST['old_pw'])){
		db_query('update user set pw=md5(?) where id=? and pw=md5(?) and name not like "%@mozat.com" limit 1',array($_POST['new_pw'],$views_uid,$_POST['old_pw']));
	}
	redirect($_PAGE['.']);
}

if(is_admin() && !empty($_GET['d_uid'])){
	$views_uid=$_GET['d_uid'];
}

$views = get_my_views($views_uid);
//var_dump($views);
$views2 = db_query_for_assoc("SELECT id, name FROM virtual_view WHERE user_id=? AND enabled>0", array($views_uid));
$groups = array();
//var_dump($views);
foreach ($views as $id => $view) {
	//echo $id, $view;
	$k = preg_replace('/\s+.*/','',$view);
	//echo $k;
	if (!$groups[$k])
		$groups[$k] = array();
	$groups[$k][] = $id;
	
}

$user_name=db_query_for_one('SELECT name FROM user WHERE id=?',array($views_uid));
if (ends_with($user_name,'@mozat.com') or is_admin()) {
	$v_view_tab = 'Personalized';
}

function get_click_history($user_id,$view_id){
	$pattern='%view.php?id='.(int)$view_id.'%';
	$history = db_query_for_assoc("SELECT max(ts),count(*) FROM log_user_access WHERE user_id=? AND url like ?", array($user_id,$pattern));
	foreach($history as $i=>$j){
		$ret=' (clicks: '.$j.', last at '.$i.') ';
	}
	return $ret;
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
<TITLE>All Views</TITLE>

<script type="text/javascript" language="javascript" src="js/jquery.js"></script>
<script type="text/javascript" language="javascript" src="js/jquery.ui.js"></script>
<script type="text/javascript" language="javascript" src="js/jquery.cookie.js"></script>
<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
		$('#tabs').tabs({cookie: { expires: 30, name: "ui-tabs-uv" }});
		if ($('#tabs').tabs('option','selected') < 0) {
			$('#tabs').tabs('option','selected', 0);
		}
	});
	function change_pw(){
		$('#old_pw').val(prompt('Please type your OLD password. (*required)'));
		if (!$('#old_pw').val()) return false;
		
		while($('#new_pw').val().length<6){
			$('#new_pw').val(prompt('Then type your NEW password. (*required, no less than 6 chars)'));
			if (!$('#new_pw').val()) return false;
		}
		$('#form1').submit();
		return false;
	}
</script>
</head>
<body>
<div style="float:right; margin-right:3px;">

<?if(ends_with($user_name,'@mozat.com')){?>

<button type="button" class="img_btn" onclick="location.href='home.php'"><img src="images/home.png" height="24" /> Home</button>

<?}?>

<button type="button" class="img_btn" onclick="location.href='logout.php'"><img src="images/logout.png" height="24"/> Logout</button>&nbsp;

</div>
<h3>All Views</h3>
<div id="tabs">
<ul>
<?
//var_dump($groups);
foreach (array_keys($groups) as $k) {?>
	<li><a href="#tabs-<?=$k?>"><span><?=q(str_replace('_',' ',$k))?></span></a></li>
<?}?>
</ul>
<?foreach ($groups as $k => $v) {?>
<div id="tabs-<?=$k?>">
<ol>
<?foreach ($v as $i=>$id) {?>
<li><a href="view.php?id=<?=$id?>"><?=substring_after($views[$id],' ')?>
</a>
<?=(!empty($_GET['d_uid']) && is_admin())?'<span style="color:gray; float:right;">'.get_click_history($_GET['d_uid'],$id).'</span>':''?></li>
<?}?>
</ol>
</div>
<?}?>
</div>

<?if(is_admin()){?>
<div class="right_link" style="clear:both;"><a href="user_management.php" target="_blank" style="color:gray;text-decoration:none;font-size:80%;">user management</a></div>
<?}?>

<?if(!ends_with($user_name,'@mozat.com')){?>
<div class="right_link" style="clear:both;"><a href="#" style="color:gray;text-decoration:none;font-size:80%;" onclick="return change_pw();">change my password</a></div>
<?}?>

<?if($user_name=='xianli@mozat.com'){?>
<div class="right_link" style="clear:both;"><a href="view.php?id=788&key1=not_exist" style="color:gray;text-decoration:none;font-size:80%;">search view</a></div>
<?}?>

<form id="form1" action="" method="POST">
<input id="old_pw" type="hidden" name="old_pw" value=""/>
<input id="new_pw" type="hidden" name="new_pw" value=""/>
<input type="hidden" name="action" value="change_pw"/>
</form>
<p></p>
</body>
</html>
