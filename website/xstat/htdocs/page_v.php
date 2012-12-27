<?php
set_time_limit(180);
require('common.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
$default_steps = 16;
$yesterday = strtotime('yesterday');
require('xstat.inc.php');
$min_width = 8 * get_steps();
if (is_post()) {
	if ($_POST['pagegroup'] == 'pchart') {
		$date2 = strtotime($_GET['date2'] = $_POST['date2']);
		$date1 = strtotime($_GET['date1'] = $_POST['date1']);
		$days = ($date2 - $date1)/SECONDS_PER_DAY + 1;
		$allow_zoom_in = $days >= $min_width;
		if ($_POST['action'] == 'zoom-out-left')
			$_GET['date1'] = date('Y-m-d', $date1 - $days*SECONDS_PER_DAY);
		elseif ($_POST['action'] == 'zoom-out-right' && $date2 < $yesterday)
			$_GET['date2'] = date('Y-m-d', $date2 + $days*SECONDS_PER_DAY);
		elseif ($_POST['action'] == 'zoom-in-left' && $allow_zoom_in)
			$_GET['date1']  = date('Y-m-d', $date1 + ((int)($days/2))*SECONDS_PER_DAY);
		elseif ($_POST['action'] == 'zoom-in-right' && $allow_zoom_in)
			$_GET['date2']  = date('Y-m-d', $date2 - ((int)($days/2))*SECONDS_PER_DAY);
	} else {
		if ($_POST['group_by'] != ors($_GET['group_by'],'')) {
			$_GET['group_by'] = $_POST['group_by'];
			$_GET['date1'] = $_GET['date2'] = '';
		} else {
			$_GET['date1'] = trim($_POST['date1']);
			$_GET['date2'] = trim($_POST['date2']);
		}
		redirect($_SERVER['SCRIPT_NAME'].'?'.http_build_query(array_unset_blank($_GET)));
	}
}
$selected1[ors($_GET['group_by'],'')] = ' selected="selected"';
$_PAGE['view_id'] = $_GET['id'];
$view = db_fetch_assoc(db_query('SELECT * FROM virtual_view WHERE id=? /*AND user_id=1*/',array($_PAGE['view_id'])));
if (!$view)
	die('access denied');
$view['opt'] = json_decode(if_blank($view['options'],'[]'), TRUE);
$views = get_my_views(sess_get_uid());
$params = get_default_params(ors($view['day_range_default'],$default_steps),$view['include_today']);
$values = $keys = $view['cols'] = array();
$sth = db_query('SELECT virtual_view_item.id,`sql`,view_id,col_name,alias,options FROM virtual_view_item, view WHERE view_id=view.id AND virtual_view_id=? ORDER BY seq',array($_PAGE['view_id']));
for ($i = 0; $row = db_fetch_assoc($sth); $i++) {
	$col = json_decode(if_blank($row['options'],'[]'),TRUE) + array(
		'name' => ors($row['alias'],$row['col_name']), 'id' => $row['id']);
	if ($views[$row['view_id']] && ($_POST['pagegroup']!='pchart' || $i>=$_POST['page'] && $i<$_POST['page']+$_POST['span']))
		$keys[$row['sql']][$i] = $row['col_name'];
	$view['cols'][] = $col;
}
if (!$view['opt']['non_time_key']) {
	if ($_POST['pagegroup'] != 'pchart')
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
	while ($row = db_fetch_assoc($sth)) {
		list($k,$t) = each($row);
		if ($_GET['group_by'])
			$values[$t][0] = $t;
		foreach ($names as $j => $name) {
			$values[$t][$j+1] = $row[$name];
			$view['cols'][$j]['first_dt'] = $t;
		}
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
require(LIB_PCHART);
list($i,$values_sum) = each(aggr_sum($values));
include('subpages/page_v.php');
if ($_POST['pagegroup'] == 'pchart') {
	output_subpage_pchart($_PAGE, $_POST['page']);
	exit(0);
}
?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta content="text/html; charset=utf-8" http-equiv="Content-Type">
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<link href="common.css" rel="stylesheet" type="text/css"/>

<style type="text/css">
	div.break { clear: both; }
	div.block { float: left; }
	div.navi_bar { position: absolute; display: none; }
	h2 { margin: 0; }

	td a {
		float:right;
		padding-right:8px;
		color:gray;
		font-size:12px;
	}
	td a:hover{
		float:right;
		padding-right:8px;
		color:blue;
		font-size:12px;
	}
</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" src="js/jquery.metadata.js"></script>
<script type="text/javascript" src="js/jquery.form.js"></script>
<script type="text/javascript" src="js/jquery.blockUI.js"></script>
<script type="text/javascript" src="js/jquery.cookie.js"></script>
<script type="text/javascript" src="common.js"></script>
<script type="text/javascript" charset="utf-8">
function on_load() {
	$('.navi_bar', this).hover(function(e) {$(this).toggle(e.type=='mouseenter')})
	.prev().hover(function(e) {
		if (e.type == 'mouseenter')
			$(this).next().css({width: $(this).width(), top: $(this).offset().top+$(this).height()-30}).show()
		else
			$(this).next().hide()
	});
	$('.zoom-in', this).hover(function(e) {
		if (e.type == 'mouseenter') {
			var $img = $(this).closest('.navi_bar').prev();
			var ml = $(this).metadata().post.action.match(/-left$/) ? $img.width()/2 : 0;
			$(this).closest('.navi_bar').next().css({top: $img.offset().top, width: $img.width()/2, height: $img.height(), marginLeft: ml}).show()
		} else
			$(this).closest('.navi_bar').next().hide()
	});
	$(':disabled img.grayscaleable', this).each(function() {
		$(this).removeClass('grayscaleable').attr('src', $(this).attr('src').replace(/\.png$/i,'-gs.png'));
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
	$('div.break', this).each(function() {
		if (!$(this).next().is('div.block'))
			$(this).hide();
	});
}
</script>
</head>
<body>
<table width="100%">
<tr><td height="5"></td></tr>
<tr><td>
<h2 style="float: left"><?=q($view['name'])?></h2>
<?if (!$view['opt']['non_time_key']) {?>
<form action="<?=$_PAGE['.']?>" method="post" class="noinit">
<div style="float: right">
<select name="group_by">
<option value=""<?=$selected1['']?>>Daily</option>
<option value="weekly"<?=$selected1['weekly']?>>Weekly</option>
<option value="monthly"<?=$selected1['monthly']?>>Monthly</option>
</select>
Date between: <input type="text" name="date1" class="datepicker {value:'<?=q($_GET['date1'])?>'}" size="8"/> <input type="text" name="date2" class="datepicker {value:'<?=q($_GET['date2'])?>'}" size="8"/>
<button type="submit" class="img_btn"><img src="images/reload.png" height="24"/> Reload</button>
<button type="button" class="img_btn" onclick="window.top.location='home.php';"><img src="images/home.png" height="24"/> Home</button>
</div>
</form>
<?}?>
</td></tr>
<tr><td height="10"></td></tr>
<tr><td>
<?for ($i = 0; $col = $view['cols'][$i]; $i += ors($col['span'],1)) {?>
<?if ($col['break']){?>
<div class="break"><?=$col['break']?></div>
<?}?>
<?if ($col['max'] || $col['min']) {?>
<div class="block" style="<?=$col['div_style']?>">
<div><?=$col['header']?></div>
<table>
<tr><td height="25"><strong><?=q(ors($col['title'],$col['name']))?></strong>
<?if (!$view['opt']['non_time_key'] && (!$col['span'] || $col['span']==1)) {?>
: <?=n($col['last'])?>&nbsp;
<?if (count($col['last_avg_3w']) == 3 && ($d = dir_of_array($col['last_avg_3w'])) < 0) {?>
<img src="images/stock-up.png" align="absmiddle" width="16" />
<?} elseif ($d > 0){?>
<img src="images/stock-down.png" align="absmiddle" width="16" />
<?}?>
<?if ($col['last'] == $col['max']) {?>
<img src="images/go-top.png" align="absmiddle" width="16"/>
<?}?> 
<?if ($col['last'] == $col['min']) {?>
<img src="images/go-bottom.png" align="absmiddle" width="16"/>
<?}?>
<?}?>
</td></tr>
<tr><td>
<div id="subpage_<?=$i?>">
<? output_subpage_pchart($_PAGE, $i); ?>
</div>
</td>
<td valign="bottom">
<?if (!$view['opt']['non_time_key'] && $col['avg_loc'] == 'right') {?>
<div style="line-height: 20px">1 week avg: <?=n(e($col['last_avg_w']))?></div>
<div style="line-height: 20px">1 month avg: <?=n(e($col['last_avg_m']))?></div>
<?}?>
<?if ($col['link_r']) {?>
<div style="line-height: 20px"><a href="<?=$col['link_r']?>">detail</a></div>
<?}?>
</td></tr>
<tr><td>
<div>
<?if (!$view['opt']['non_time_key'] && $col['avg_loc'] == 'bottom') {?>
1 week avg: <?=n(e($col['last_avg_w']))?>&nbsp;&nbsp;1 month avg: <?=n(e($col['last_avg_m']))?>&nbsp;&nbsp;
<?}?>
<?if ($col['link_b']) {?>
<a href="<?=$col['link_b']?>">detail</a>
<?}?>
</div>
<div><?=$col['descr']?></div>
<br/>
</td></tr>
</table>
</div>
<?} //if ($col['max'] || $col['min'])?>
<?=$col['after']?>
<?}?>
</td></tr>
<tr><td height="30"></td></tr>
</table>
</body>
</html>