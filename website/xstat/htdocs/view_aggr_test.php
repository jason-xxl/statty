<?php
set_time_limit(720);
require('common.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');
$edit_mode=$_GET['edit']==1?True:False;

$view_id = $_GET['id'];
if (!test_access($view_id))
	die('access denied');
$view = db_fetch_assoc(db_query('SELECT * FROM view WHERE view.id=?',array($view_id)));
if (is_post()) {
	$_GET['date1'] = trim($_POST['date1']);
	$_GET['date2'] = trim($_POST['date2']);
	redirect($_SERVER['SCRIPT_NAME'].'?'.http_build_query($_GET));
} else {
	$diff= $view['day_range_default'] ? '-'.($view['day_range_default']-1).' days' : '-1 month';
}

$values = $cols = $charts = array();

$params = get_default_params($diff);



$sql=getAvgSql($view['sql'],$params,'seasonly');
$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2',$sql);

$data_sql=$sql;

//echo $data_sql;
//exit(0);

$sth = db_query($sql, $params);

//var_dump($params);

if ($row = db_fetch_assoc($sth)) {
	$cols = array_keys($row);
	$values[] = array_values($row);
	while ($row = db_fetch_array($sth)) {
		$values[] = $row;
	}
}

if ($cols) {
	require(LIB_PCHART);
	$sql = 'SELECT * FROM chart WHERE view_id=? ORDER BY tab_order';
	$sth = db_query($sql,array($view_id));
	while ($row = db_fetch_assoc($sth)) {
		$legends = get_legends(array_flip($cols),explode(',',$row['columns']));
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
<html>
<head>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>

<link type="text/css" href="css/multiselect/ui.multiselect.css" rel="stylesheet" />

<style type="text/css">
	#tabs { font-size:70%; }
	th,td { font-size: 8pt; }
	.main_table { border-collapse: collapse; background-color: white; }
	.main_table td { white-space: nowrap; text-align:right; padding:3px; }
	.main_table tbody tr:nth-child(odd) { background-color: #EEE; } /* IE not support */
	.chart_title { text-align: right; padding-right: 5px; }
	.chart_block { margin-bottom: 10px; }
	.chart_descr { padding: 15px 10px; }
	.view_descr { padding: 15px 0px; }
	.right_link { margin-top: 5px; float:right;}
	.highlight { background-color: #FFFF96!important; }
	.highlight2 { background-color: #FFFF96!important; }

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

<script type="text/javascript" src="js/multiselect/plugins/localisation/jquery.localisation-min.js"></script>
<script type="text/javascript" src="js/multiselect/plugins/scrollTo/jquery.scrollTo-min.js"></script>
<script type="text/javascript" src="js/multiselect/ui.multiselect.js"></script>


<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
		if ($.browser.msie) {
			$('.main_table tbody tr:nth-child(odd)').css('backgroundColor','#EEE');
		}
		var tabs = $('#tabs').tabs({cookie: { expires: 30, name: "ui-tabs-v" ]]);
		//$.localise('ui-multiselect', {/*language: 'en',*/ path: 'js/locale/'});
		$(".multiselect").multiselect();

		$(".main_table tbody tr").click(function() {
			$(this).toggleClass('highlight');
		});
		$(".main_table th").click(function() {
				$(".main_table tr :nth-child("+($(this).index()+1)+")").toggleClass('highlight2');
		});

	});
</script>
</head>
<body>

<?if ($_GET['d']==1) {
	foreach ($params as $k=>$v){
		$data_sql=str_replace(':'.$k,$v,$data_sql);
	}
	?>
	<code>
	<?=nl2br(htmlentities($data_sql))?>
	</code>
	<code>
	<?
	var_dump($params);
	?>
	</code>
<?}?>

<div style="float:right; margin-right:3px;"><input class="groovybutton" type="button" onclick="location.href='user_view.php';" style="font-size:12px; text-decoration:none;" value="Back" /></div>
<h3>
<?
$title_group=explode(' ',$view['name'],2);
echo(q(str_replace('_',' ',$title_group[0])));
echo(' - ');
echo(q(str_replace('_',' ',$title_group[1])));

?></h3>

<div id="tabs">
<ul>
	<li><a href="#tabs-1"><span>Table</span></a></li>
<?foreach ($charts as $i => $chart) {?>
	<li><a href="#tabs-c<?=$i+1?>"><span><?=q($chart['name'])?></span></a></li>
<?}?>
	<li><a href="#tabs-3"><span>Export</span></a></li>
</ul>
<div id="tabs-1">
<table border="1" class="main_table" style="margin-top:3px;margin-bottom:3px;">
<thead>
<tr>
<?foreach ($cols as $k) {?>
<th><?=$k?></th>
<?}?>
</tr>
</thead>
<tbody>
<?foreach ($values as $i => $row) {?>
<tr>
<?foreach ($row as $v) {?>
<td><?=n($v)?>&nbsp;</td>
<?}?>
</tr>
<?}?>
</tbody>
</table>
<?
if ($view['description']){
?>
<div class="view_descr"><?=nl2br($view['description'])?></div>
<?
}
?>

<?if(is_admin()){?>
<div class="right_link"><a href="#" onclick="window.open('chart_create.php?&id=<?=$view_id?>');return false;" target="_blank" style="color:gray;text-decoration:none;">create chart</a>
</div>
<div style="clear:both;"></div>
<?}?>

</div>
<?foreach ($charts as $i => $chart) {?>
<div id="tabs-c<?=$i+1?>">

<?
//var_dump($options);
//var_dump($chart);
//var_dump($_POST);


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


<?if(is_admin()){?>
<div class="right_link"><a href="#" onclick="window.open('chart_setting.php?chart_id=<?=$chart['id']?>&id=<?=$chart['view_id']?>');return false;" target="_blank" style="color:gray;text-decoration:none;">chart setting</a>
</div>
<?}?>


</span>
<?}?>
</div>
<?}?>
<div id="tabs-3">
<div><a href="excel.php?id=<?=$view_id?>&date1=<?=$_GET['date1']?>&date2=<?=$_GET['date2']?>"><img src="images/excel.png" border="0"/></a></div>
</div>
</div>

<div style="clear:both;"></div>


<div style="font-size:85%;">

<div style="float:left;">
<form action="<?=$_PAGE['.']?>" method="post" style="margin-top:12px;">

Date between: <input type="text" name="date1" value="<?=q($_GET['date1'])?>"/> <input type="text" name="date2" value="<?=q($_GET['date2'])?>"/>
<input class="groovybutton" type="submit" value="Reload"/>
</form>

</div>
</div>

</body>
</html>