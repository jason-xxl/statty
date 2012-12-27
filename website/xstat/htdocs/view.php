<?php
set_time_limit(720);
require('common.inc.php');
require('sql_preprocess.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
$default_steps = $_GET['group_by'] ? 20 : idate('t',strtotime('-1month'));
$yesterday = strtotime('yesterday');
require('xstat.inc.php');
include('subpages/view.php');
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
if (!test_access($_PAGE['view_id']))
	die('access denied');
$view = db_fetch_assoc(db_query('SELECT * FROM view WHERE view.id=?',array($_PAGE['view_id'])));
$values = $view['cols'] = $view['opt'] = $charts = array();
$params = get_default_params(ors($view['day_range_default'],$default_steps),$view['include_today'],$view['default_start_date']);
$sql = $_GET['group_by']?getAggregatingSql($view['sql'],$params,$_GET['group_by'],$_GET['id']):$view['sql'];

#$sql_debug = str_replace('%where_and_more_compact%', ' AND `date`>=convert(replace(:date1,\'-\',\'\'),DECIMAL) AND `date`<=convert(replace(:date2,\'-\',\'\'),DECIMAL)', $sql_debug);

$sql_debug = str_replace('%where_and_more_range%', ' AND date >= :date1 AND date <= :date2', $sql);
$sql_debug = str_replace('%where_and_more%', ' AND date in ('.$params['dates_str_str'].')', $sql_debug);
$sql_debug=str_replace('%where_and_more_compact%', ' AND date in ('.$params['dates_int_str'].')', $sql_debug);

$sql = $sql_debug;

$sql=replace_get_md5_key($sql);
$sql=replace_get_md5_sub_key($sql);

$sql_display=$sql;

$sth = db_query($sql, $params,$view['conn_string']);

if ($row = db_fetch_assoc($sth)) {
	$view['cols'] = array_keys($row);
	if (!preg_match('/^(Time|Date)$/i', $view['cols'][0]))
		$view['opt']['non_time_key'] = TRUE;
	if ($_GET['unfold_collection']==1){
		//var_dump($row);
		if(!empty($row)){
			foreach($row as $kr=>$vr){
				$elements=explode(',',$vr);
				foreach($elements as $e){
					$values[] = array($e);
				}
				//break;
			}
		}
	}else{
		$values[] = array_values($row);
		while ($row = db_fetch_array($sth))
			$values[] = $row;
	}
}
$user_name = db_query_for_one('SELECT name FROM user WHERE id=?',array(sess_get_uid()));
if ($_POST['page'] == '#tabs-1' && ends_with($user_name, '@mozat.com')) {
	$notes = "<span class=\"user_name\">$user_name</span>:\n<span class=\"content\">".trim($_POST['notes'])."</span>";
	if ($_POST['action'] == 'add')
		db_update('INSERT IGNORE INTO notes (mkey,notes) VALUES (?,?)', array($_POST['mkey'],$notes));
	elseif ($_POST['action'] == 'delete')
		db_update('DELETE FROM notes WHERE mkey=?', array($_POST['mkey']));
	elseif ($_POST['action'] == 'append')
		db_update('UPDATE notes SET notes=CONCAT(notes,?) WHERE mkey=?', array("<hr/>".$notes,$_POST['mkey']));
	output_subpage_table(&$_PAGE);
	exit(0);
} else {
	require(LIB_PCHART);
	$sql = 'SELECT * FROM chart WHERE view_id=? ORDER BY tab_order';
	$sth = db_query($sql,array($_PAGE['view_id']));
	while ($row = db_fetch_assoc($sth)) {
		$legends = get_legends(array_flip($view['cols']),explode(',',$row['columns']));
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
?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<link href="common.css" rel="stylesheet" type="text/css"/>
<title><?=$view['name']?></title>
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
	.commented { background-color: #A1C6F4; cursor: default; }
	#add_comment, #edit_comment { position: absolute; padding: 0px 4px; margin-top: -2px; display: none; }
	#comment_add, #comment_append, #add_comment, #edit_comment { display: none; }
	h2 { margin: 0; }
	pre { font-size: 1.2em; word-wrap: break-word; margin-top: 0px; }
</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" src="js/jquery.metadata.js"></script>
<script type="text/javascript" src="js/jquery.validate.js"></script>
<script type="text/javascript" src="js/jquery.form.js"></script>
<script type="text/javascript" src="js/jquery.blockUI.js"></script>
<script type="text/javascript" src="js/jquery.cookie.js"></script>
<script type="text/javascript" src="common.js"></script>

<script type="text/javascript" src="./highchart/js/highcharts.js"></script>
<script type="text/javascript" src="./highchart/js/modules/exporting.js"></script>

<script type="text/javascript" charset="utf-8">
function on_load() {
	if (!$(this).is('.ui-dialog-content'))
		$('.ui-dialog-content').unblock().dialog('close');
	if ($.browser.msie) {
		$('.main_table tbody tr:nth-child(odd)',this).css('backgroundColor','#EEE');
	}
	$('#tabs', this).tabs({cookie: { expires: 30, name: "ui-tabs-v" }});
	$(".main_table tbody tr", this).click(function() {
		$(this).toggleClass('highlight');
	});
	$(".main_table th", this).click(function() {
		$(".main_table tr :nth-child("+($(this).index()+1)+")").toggleClass('highlight2');
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
	if ($.browser['mozilla'] && $.browser['version']=='12.0')
	{
		$('#tabs').css('overflow-x','auto');
	}

	$('#ui-datepicker-div').css('display','none');	
};
function getParameterByName(name) {
	var match = RegExp('[?&]' + name + '=([^&]*)').exec(window.location.search);
	return match && decodeURIComponent(match[1].replace(/\+/g, ' '));
}
</script>
</head>
<body>
<?if ($_GET['d']==1) {
	foreach ($params as $k=>$v){
		$sql_display=str_replace(':'.$k,$v,$sql_display);
	}
	?>
	<textarea style="width: 100%; height: 400px"><?=htmlentities($sql_display, ENT_QUOTES | ENT_IGNORE, "UTF-8")?></textarea>
	<div>connection: <?=htmlentities($view['conn_string'])?></div>
<?}?>
<div style="clear: both"><img src="images/blank.png" height="5"/></div>
<h2 style="float: left"><?
	$title_str=	strtr(preg_replace('/ /',' - ',$view['name'],1),'_',' ');
	if($_GET['group_by']=='weekly'){
		$title_str=	preg_replace('/Daily$/','Weekly',$title_str,1);
	}else if($_GET['group_by']=='monthly'){
		$title_str=	preg_replace('/Daily$/','Monthly',$title_str,1);
	}
	?><?=$title_str?></h2>
<div style="float: right">
<form action="<?=$_PAGE['.']?>" method="post" class="noinit">
<?if (!$view['opt']['non_time_key']) {?>
<?if (!preg_match('/Hourly$/i', $view['name'])) {?>
<select name="group_by" style="display:<?=$view['aggregatable']?'':'none'?>;">
<option value=""<?=$selected1['']?>>Daily</option>

<?
	
$view_weekly = db_fetch_assoc(db_query('SELECT * FROM view_weekly WHERE view_weekly.id=?',array($_PAGE['view_id'])));
$view_monthly = db_fetch_assoc(db_query('SELECT * FROM view_monthly WHERE view_monthly.id=?',array($_PAGE['view_id'])));
if(empty($view_weekly) && empty($view_monthly)){
	$view_weekly=True;
	$view_monthly=True;
}

if(!empty($view_weekly)){
	?><option value="weekly"<?=$selected1['weekly']?>>Weekly</option><?
}
if(!empty($view_monthly)){
	?><option value="monthly"<?=$selected1['monthly']?>>Monthly</option><?
}
?>
</select>
<?}?>
Date between: <input type="text" name="date1" class="datepicker {value:'<?=q($_GET['date1'])?>'}" size="8"/> <input type="text" name="date2" class="datepicker {value:'<?=q($_GET['date2'])?>'}" size="8"/>
<button type="submit" class="img_btn"><img src="images/reload.png" height="24"/> Reload</button>
<?}?>
<button type="button" onclick="location.href='user_view.php';" class="img_btn"><img src="images/back.png" width="24" border="0"/> Directory</button>
</form>
</div>
<div style="clear: both"><img src="images/blank.png" height="5"/></div>
<div id="tabs">
<ul>
	<li><a href="#tabs-1"><span>Table</span></a></li>
<?foreach ($charts as $i => $chart) {?>
	<li><a href="#tabs-c<?=$i+1?>"><span><?=q($chart['name'])?></span></a></li>
<?}?>

	<li><a href="#tabs-3"><span>Export</span></a></li>

</ul>
<div id="tabs-1">

<script type="text/javascript">
if (window.location.toString().indexOf('sub_title=')>-1) {
	window.location.toString().match(/sub_title=([\w\s]*)/ig);
	document.write('<h4>'+RegExp.$1.replace(/_/ig,'&nbsp;')+'<div style="float:right;"><a href="?id=<?=$_GET['id']?>" style="color:gray; text-decoration:none;">[Back]</a></div></h4>');
}
</script>

<? output_subpage_table($_PAGE); ?>
</div>
<?foreach ($charts as $i => $chart) {?>
<div id="tabs-c<?=$i+1?>">
<div style="clear:both"></div>
<iframe id="form-c<?=$i+1?>" style="width:90%;height:800px;display:none;border:0;"></iframe>
<?foreach ($chart['charts'] as $k => $h) {?>
<span style="display: inline-block;" class="chart_block">
<?if (is_string($k)) {?>
<div class="chart_title"><?=$k?></div>
<?}?>
<img src="chart.php?h=<?=$h?>"/>
<div class="chart_descr"><?=q($chart['description'])?></div>
<?if(is_admin()){?>
<div class="right_link"><a href="#" onclick="window.open('chart_setting.php?chart_id=<?=$chart['id']?>&id=<?=$chart['view_id']?>');return false;" target="_blank" style="color:gray;text-decoration:none;">chart setting</a>
</div>
<?}?>
</span>
<?}?>
</div>
<?}?>

<div id="tabs-3">
<div><a href="excel.php?<?=$_SERVER['QUERY_STRING'] ?>&date1=<?=$_GET['date1']?>&date2=<?=$_GET['date2']?>"><img src="images/excel.png" border="0" width="24"/></a>
<?if(empty($view['conn_string'])){?>
&nbsp;&nbsp;<a href="export.php?<?=$_SERVER['QUERY_STRING'] ?>&date1=<?=$_GET['date1']?>&date2=<?=$_GET['date2']?>&s=%09"><img src="images/text-24x24.png" border="0" width="24"/></a>
&nbsp;&nbsp;<a href="export_xml.php?<?=$_SERVER['QUERY_STRING'] ?>&date1=<?=$_GET['date1']?>&date2=<?=$_GET['date2']?>&s=%09"><img src="images/xml-32x32.png" border="0" width="24"/></a></div>
<?}?>
</div>

</div>
<script type="text/javascript" src="./highchart/helper_highchart.js"></script>
<script type="text/javascript"><?=$view['charting_javascript']?></script>

</body>
</html>
