<?php
set_time_limit(180);
require('common.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
$default_steps = $_GET['group_by'] ? 20 : idate('t',strtotime('-1month'));
$yesterday = strtotime('yesterday');
require('xstat.inc.php');
include('subpages/view_v.php');
if (is_post() && !$_POST['page']) {
	if ($_POST['group_by'] != ors($_GET['group_by'],'')) {
		$_GET['group_by'] = $_POST['group_by'];
		$_GET['date1'] = $_GET['date2'] = '';
	} else {
		$_GET['date1'] = trim($_POST['date1']);
		$_GET['date2'] = trim($_POST['date2']);
	}
	redirect($_SERVER['SCRIPT_NAME'].'?'.http_build_query(array_unset_blank($_GET)));
}
$selected1[ors($_GET['group_by'],'')] = ' selected="selected"';
$_PAGE['view_id'] = $_GET['id'];

if(is_admin()){
	$view = db_fetch_assoc(db_query('SELECT * FROM virtual_view WHERE id=?',array($_PAGE['view_id'])));
}else{
	$view = db_fetch_assoc(db_query('SELECT * FROM virtual_view WHERE id=? AND user_id=?',array($_PAGE['view_id'],sess_get_uid())));
}

if (!$view)
	die('access denied');

$view['opt'] = json_decode(if_blank($view['options'],'[]'), TRUE);
$views = get_my_views(sess_get_uid());
$params = get_default_params(ors($view['day_range_default'],$default_steps),$view['include_today']);
$values = $view['cols'] = $keys = $charts = array();
$sth = db_query('SELECT virtual_view_item.id,`sql`,view_id,col_name,alias FROM virtual_view_item, view WHERE view_id=view.id AND virtual_view_id=? ORDER BY seq',array($_PAGE['view_id']));
for ($i = 0; $row = db_fetch_assoc($sth); $i++) {
	if ($views[$row['view_id']])
		$keys[$row['sql']][$i] = $row['col_name'];
	$view['cols'][] = array('name'=>ors($row['alias'],$row['col_name']), 'id'=>$row['id']);
}
if (!$view['opt']['non_time_key']) {
	$params2 = array('date2' => date('Y-m-d',$yesterday), 'date1' => date('Y-m-d',strtotime('-99 days',$yesterday))) + $params;
	if (!$_GET['group_by']) {
		for ($i = strtotime($_GET['date2']); ($t=date('Y-m-d',$i)) >= $_GET['date1']; $i-=SECONDS_PER_DAY)
			$values[$t] = array($t);
	}
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
	if ($params2) {
		$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $s);
		$sth = db_query($sql, $params2);
		for ($max_dt = 0; $row = db_fetch_assoc($sth);) {
			list($k,$t) = each($row);
			$max_dt || ($max_dt = strtotime($t));
			foreach ($names as $j => $name)
				aggregate_col($view['cols'][$j], $row[$name], ($max_dt-strtotime($t))/SECONDS_PER_DAY, $t);
		}
	}
}
$user_name = db_query_for_one('SELECT name FROM user WHERE id=?',array(sess_get_uid()));
if ($_POST['page'] == '#tabs-1' && ends_with($user_name, '@mozat.com')) {
	$notes = $user_name.":\n".trim($_POST['notes']);
	if ($_POST['action'] == 'add')
		db_update('INSERT IGNORE INTO notes (mkey,notes) VALUES (?,?)', array($_POST['mkey'],$notes));
	elseif ($_POST['action'] == 'delete')
		db_update('DELETE FROM notes WHERE mkey=?', array($_POST['mkey']));
	elseif ($_POST['action'] == 'append')
		db_update('UPDATE notes SET notes=CONCAT(notes,?) WHERE mkey=?', array("\n".$notes,$_POST['mkey']));
	output_subpage_table(&$_PAGE);
	exit(0);
} else {
	require(LIB_PCHART);
	$keys_map = array($view['key']=>0);
	foreach ($view['cols'] as $i => &$_col) {
		$_col['chart'] = to_2d_chart_data($values, '2d4', array($_col['name'] => $i + 1));
		$keys_map[$_col['name']] = $i + 1;
	}
	$sql = 'SELECT * FROM virtual_view_chart WHERE view_id=? ORDER BY tab_order';
	$sth = db_query($sql,array($_PAGE['view_id']));
	while ($row = db_fetch_assoc($sth)) {
		$legends = get_legends($keys_map,explode(',',$row['columns']));
		$row['charts'] = array();
		$options = json_decode(if_blank($row['options'],'[]'),TRUE);
		$type = $options['Type'];
		$limit = ors($options['RowLimit'], 0x7FFFFFFF);
		$template = ors($options['Template'], array());
		$a = $options['Aggregation'] == 'sum' ? aggr_sum($values) : array_slice($values,0,$limit);
		if (starts_with($type, '1d'))
			foreach ($a as $v)
				$row['charts'][$v[0]] = to_1d_chart_data($v, $type, $legends, $template);
		elseif (starts_with($type, '2d'))
			$row['charts'][] = to_2d_chart_data($a, $type, $legends, $template);
		if ($row['charts'])
			$charts[] = $row;
	}
}
$help_url = 'http://wiki.mozat.com/en/doku.php?id=project:statisticportal:website_detail';
?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta content="text/html; charset=utf-8" http-equiv="Content-Type">
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<link href="common.css" rel="stylesheet" type="text/css"/>
<title><?=$view['name']?> - My View <?=$view['id']?></title>
<style type="text/css">
	.main_table { background-color: white; margin:3px 0px; }
	.main_table td { white-space: nowrap; text-align:right; padding:3px; }
	.main_table tbody tr:nth-child(odd) { background-color: #EEE; } /* IE not support */
	.chart_title { text-align: right; padding-right: 5px; }
	.chart_block { margin-bottom: 10px; }
	.chart_descr { padding: 15px 10px; }
	.view_descr { padding: 15px 0px; }
	.right_link { margin-top: 5px; float:right;}
	.highlight { background-color: #FFFF96!important; }
	.highlight2 { background-color: #FFFF96!important; }
	.highlight3 { background-color: #9DB6DF!important; }
	.commented { background-color: #A1C6F4; cursor: default; }
	#add_comment, #edit_comment { position: absolute; padding: 0px 4px; margin-top: -2px; display: none; }
	#comment_add, #comment_append, #add_comment, #edit_comment { display: none; }
	caption { text-align: left; }
	h2 { margin: 0; }
	pre { font-size: 1.2em; word-wrap: break-word; margin-top: 0px; }
</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" src="js/jquery.metadata.js"></script>
<script type="text/javascript" src="js/jquery.form.js"></script>
<script type="text/javascript" src="js/jquery.validate.js"></script>
<script type="text/javascript" src="js/jquery.blockUI.js"></script>
<script type="text/javascript" src="js/jquery.cookie.js"></script>
<script type="text/javascript" src="common.js"></script>

<script type="text/javascript" charset="utf-8">
function on_load() {
	if (!$(this).is('.ui-dialog-content'))
		$('.ui-dialog-content').unblock().dialog('close');
	if ($.browser.msie) {
		$('.main_table tbody tr:nth-child(odd)', this).css('backgroundColor','#EEE');
	}
	$('#tabs', this).tabs({cookie: { expires: 30, name: "ui-tabs-v" }});
	$(".main_table tbody tr", this).click(function() {
		$(this).toggleClass('highlight');
	});
	$(".main_table th", this).click(function() {
		$(".main_table tr :nth-child("+($(this).index()+1)+")").toggleClass('highlight2');
	});
	$("#tabs a.tabs-link", this).click(function() {
		$('#tabs').tabs('select', '#tabs-1');
		$(".highlight3").removeClass("highlight3");
		$(".main_table tr :nth-child("+($(this).metadata().index+1)+")").addClass('highlight3');
		return false;
	});
	$("td.commentable", this).hover(function(e) {
		if (e.type == 'mouseenter')
			$(this).append($('#add_comment').show())
		else
			$('#add_comment').hide();
	});
	$("td.commented", this).hover(function(e) {
		if (e.type == 'mouseenter')
			$(this).append($('#edit_comment').show())
		else
			$('#edit_comment').hide();
	});
	$('#add_comment>button', this).click(function() {
		var k = $('#comment_add :submit').metadata().post.mkey = $(this).closest('tr').find('td:first-child').metadata().mkey;
		$('#comment_add #comment_date').text(k);
		$('#comment_add').dialog({
			modal: true, width: 550, height: 400,
			close: function() { $('form', this).validate().resetForm(); }
		});
		return false;
	});
	$('#edit_comment>:submit', this).click(function() {
		$(this).metadata().post.mkey = $(this).closest('tr').find('td:first-child').metadata().mkey;
		return true;
	});
	$('#edit_comment>button:not(:submit)',this).click(function() {
		var k = $('#comment_append :submit').metadata().post.mkey = $(this).closest('tr').find('td:first-child').metadata().mkey;
		$('#comment_append #comment_date').text(k);
		$('#comment_append #comment_content').html($(this).closest('tr').find('td:first-child>span').text());
		$('#comment_append').dialog({
			modal: true, width: 550, height: 400,
			close: function() { $('form', this).validate().resetForm(); }
		});
		return false;
	});
	$('.datepicker', this).datepicker({dateFormat: 'yy-mm-dd'}).each(function() {
		$(this).datepicker("setDate", $(this).metadata().value );
	});
	$('form.noinit', this).submit(function() {
		$.blockUI($.defaultLoader);
	});
	$('select[name=group_by]', this).change(function() {
		$(this.form).submit();
	});
}
</script>
</head>
<body>
<div style="clear: both"><img src="images/blank.png" height="5"/></div>
<h2 style="float: left"><?=q($view['name'])?></h2>
<div style="float: right">
<form action="<?=$_PAGE['.']?>" method="post" class="noinit">
<?if (!$view['opt']['non_time_key']) {?>
<select name="group_by">
<option value=""<?=$selected1['']?>>Daily</option>
<option value="weekly"<?=$selected1['weekly']?>>Weekly</option>
<option value="monthly"<?=$selected1['monthly']?>>Monthly</option>
</select>
Date between: <input type="text" name="date1" class="datepicker {value:'<?=q($_GET['date1'])?>'}" size="8"/> <input type="text" name="date2" class="datepicker {value:'<?=q($_GET['date2'])?>'}" size="8"/>
<button type="submit" class="img_btn"><img src="images/reload.png" height="24"/> Reload</button>
<?}?>
<button type="button" onclick="location.href='user_view_v.php';" class="img_btn"><img src="images/back.png" width="24" border="0"/> Directory</button>
</form>
</div>
<div style="clear: both"><img src="images/blank.png" height="5"/></div>
<div id="tabs">
<ul>
	<li><a href="#tabs-1"><span>Table</span></a></li>
	<li><a href="#tabs-2"><span>Trend</span></a></li>
<?foreach ($charts as $i => $chart) {?>
	<li><a href="#tabs-c<?=$i+1?>"><span><?=q($chart['name'])?></span></a></li>
<?}?>
	<li><a href="#tabs-3"><span>Export</span></a></li>
</ul>
<div id="tabs-1">
<? output_subpage_table($_PAGE); ?>
</div>
<div id="tabs-2">
<div align="right"><a class="img_link" href="<?=$help_url?>" target="_new"><img src="images/help.png" width="16"/><span>Help</span></a></div>
<?foreach ($view['cols'] as $i => $col) {?>
<table>
<caption><strong><?=q($col['name'])?></strong>
<?if (!$view['opt']['non_time_key']) {?>
: <?=n($col['last'])?>&nbsp;
<?if (count($col['last_avg_3w']) == 3 && ($ii = dir_of_array($col['last_avg_3w'])) < 0) {?>
<img src="images/stock-up.png" align="absmiddle" width="16" />
<?} elseif ($ii > 0){?>
<img src="images/stock-down.png" align="absmiddle" width="16" />
<?}?>
<?if ($col['last'] < $col['last_avg_w']) {?>
<img src="images/arrow_down.gif" align="absmiddle" height="16"/>
<?=$col['last'] <= $col['last_avg_w']*0.7?'<b>!</b>':''?>
<?}?> 
<?if ($col['last'] > $col['last_avg_w']) {?>
<img src="images/arrow_up.gif" align="absmiddle" height="16"/>
<?=$col['last'] >= $col['last_avg_w']*1.3?'<b>!</b>':''?>
<?}?> 
<?if ($col['last'] == $col['max']) {?>
<img src="images/go-top.png" align="absmiddle" width="16"/>
<?}?> 
<?if ($col['last'] == $col['min']) {?>
<img src="images/go-bottom.png" align="absmiddle" width="16"/>
<?}?>
<?}?>
</caption>
<tr><td>
<img src="chart.php?h=<?=$col['chart']?>"/>
</td>
<td valign="bottom">
<? if (!$view['opt']['non_time_key']) {?>
<div style="line-height: 20px">1 week avg: <?=n(e($col['last_avg_w']))?></div>
<div style="line-height: 20px">1 month avg: <?=n(e($col['last_avg_m']))?></div>
<?}?>
<div style="line-height: 20px"><a href="#tabs-1" class="tabs-link {index:<?=$i?>}">detail</a></div>
</td></tr>
</table>
<br/>
<?}?>
</div>
<?foreach ($charts as $i => $chart) {?>
<div id="tabs-c<?=$i+1?>">

<?

$size=explode('x',$options['Template']['Size']);
?>


<div style="clear:both"></div>
<iframe id="form-c<?=$i+1?>" style="width:90%;hright:800px;display:none;border:0;"></iframe>



<?foreach ($chart['charts'] as $k => $h) {?>
<span style="display: inline-block;" class="chart_block">

<?if (is_string($k)) {?>
<div class="chart_title"><?=$k?></div>
<?}?>

<img src="chart.php?h=<?=$h?>"/>
<div class="chart_descr"><?=q($chart['description'])?></div>



</span>
<?}?>
</div>
<?}?>
<div id="tabs-3">
<div><a href="excel_v.php?id=<?=$_PAGE['view_id']?>&group_by=<?=$_GET['group_by']?>&date1=<?=$_GET['date1']?>&date2=<?=$_GET['date2']?>"><img src="images/excel.png" border="0"/></a></div>
</div>
</div>

</body>
</html>