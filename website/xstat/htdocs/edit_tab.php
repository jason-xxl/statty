<?php
set_time_limit(180);
require('common.inc.php');
page_start();
if (!sess_get_uid()) {
	if (is_post())
		stop('Session timeout!');
	redirect('login.php');
}
$_PAGE['email'] = db_query_for_one('SELECT email FROM user WHERE id=?',array(sess_get_uid()));
require('xstat.inc.php');
include('subpages/edit_tab.php');
if (is_post()) {
	if ($_POST['action'] == 'add') {
		try {
			db_update('INSERT INTO virtual_view SET name=?, description=?, day_range_default=?, user_id=?',
				array(trim($_POST['name']), trim($_POST['description']), trim($_POST['day_range_default']), sess_get_uid()));
			if ($view_id = db_last_insert_id())
				js_redirect('edit_view_v.php?id='.$view_id);
		} catch (PDOException $err) {
			if ($err->errorInfo[1]==1062)
				$_PAGE['page_msg'][] = $e = array('type'=> 'error','text'=>'Creating view failed: duplicate name!');
		}
	} elseif ($_POST['action'] == 'delete') {
		try {
			db_update('DELETE virtual_view, virtual_view_item, virtual_view_chart FROM virtual_view LEFT JOIN virtual_view_item ON virtual_view_id=virtual_view.id LEFT JOIN virtual_view_chart ON virtual_view_chart.view_id=virtual_view.id WHERE virtual_view.id=? AND user_id=?',array($_POST['view_id'], sess_get_uid()));
			$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'View removed!');
		} catch (PDOException $err) {
		}
	} elseif ($_PAGE['email'] && (($v = $_POST['action'] == 'subscribe') || $_POST['action'] == 'unsubscribe')) {
		try {
			db_update('UPDATE virtual_view SET subscribed=? WHERE id=? AND user_id=?', array($v?1:0,$_POST['view_id'], sess_get_uid()));
			$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>"View $_POST[action]d!");
		} catch (PDOException $err) {
		}
	}
	output_subpage_body($_PAGE);
	exit(0);
}
?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<link href="common.css" rel="stylesheet" type="text/css"/>
<style type="text/css">
body { width: 800px; }
fieldset { margin-bottom: 2px; }
tbody.border td, tbody.border th { border: 1px solid gray; }
</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" src="js/jquery.metadata.js"></script>
<script type="text/javascript" src="js/jquery.validate.js"></script>
<script type="text/javascript" src="js/jquery.form.js"></script>
<script type="text/javascript" src="js/jquery.blockUI.js"></script>
<script type="text/javascript" src="js/jquery.corner.js"></script>
<script type="text/javascript" src="common.js"></script>
<script type="text/javascript">
function on_load() {
	if ($('.page_msg > *',this).size()  > 0) {
		$('.page_msg',this).fadeIn('slow')
		$('.page_msg',this).delay(2000).fadeOut('slow');
	}
}
</script>
</head>
<body>
<fieldset>
<legend><h2>Tab Customization</h2></legend>
<div id="subpage_views">
<? output_subpage_body($_PAGE); ?>
</div>
<br/>
<form action="<?=$_PAGE['.']?>" method="post">
<table cellpadding="3">
<caption><h3>Create View</h3></caption>
<tbody>
<tr><td>Name</td><td><input type="text" name="name" class="required" value="<?=q($_POST['name'])?>" maxlength="128"/></td></tr>
<tr><td>Description</td><td><textarea name="description" cols="50"><?=q($_POST['description'])?></textarea></td></tr>
<tr><td>Default range</td><td><input type="text" name="day_range_default" value="<?=ors(q($_POST['day_range_default']),0)?>" size="3" class="number"/>days
<span class="footnote">(system default: 0)</span><span for="day_range_default"></span></td></tr>
<tr valign="top"><td></td>
<td align="right"><button type="submit" class="img_btn {post:{action:'add'}}"><img src="images/add.png"/> Create</button></td></tr>
</tbody>
</table>
</form>
<br/>
</fieldset>
<div align="right"><a href="user_view_v.php" class="img_btn"><img src="images/back.png" width="24" border="0"/> Back</a></div>
<div class="page_msg"><?=get_ajax_msgs($_PAGE['page_msg'])?></div>
</body>
</html>