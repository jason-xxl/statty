<?php
set_time_limit(180);
require('common.inc.php');
require('xstat.inc.php');
require(LIB_PCHART);
page_start();
if (!is_admin())
	die('access_denied');
$default_steps = $_GET['group_by'] ? 20 : idate('t',strtotime('-1month'));

if ($_REQUEST['action'] && is_admin()) {
	if($_REQUEST['action'] == 'save_chart_setting') {
		$option=array(
			"Type"=>$_REQUEST['chart_type']?$_REQUEST['chart_type']:'1d1',
			"RowLimit"=>$_REQUEST['chart_row_limit']?$_REQUEST['chart_row_limit']:0,
			"Aggregation"=>$_REQUEST['chart_aggregation']
				?$_REQUEST['chart_aggregation']:'',

			"Template"=>array(
				"DataDescription"=>array(
					"Format"=>array("Y"=>"metric")
				),
				"Options"=>array("scaleStart0"=>$_REQUEST['chart_start_zero']==='0'
					?$_REQUEST['chart_start_zero']:'1'),

				"Font"=>8,
				"Size"=>($_REQUEST['chart_size_width']?$_REQUEST['chart_size_width']:'1000')
					.'x'.
					($_REQUEST['chart_size_height']?$_REQUEST['chart_size_height']:'400')
			)
		);
		$optionStr=json_encode($option);


		db_query("update chart set name=?, description=?, columns=?, options=? where id=?", array(
			$_REQUEST['chart_name'],
			$_REQUEST['chart_description'],
			implode(',',$_REQUEST['chart_columns']),
			$optionStr,
			$_REQUEST['id']
		));

		echo "<script>window.opener.location.reload();window.opener = top;window.close();</script>";
		exit();
		//redirect($_PAGE['.']);
	}else if($_REQUEST['action'] == 'delete'){
		db_query("delete from chart where id=?", array(
			$_REQUEST['chart_id']
		));

		echo "<script>window.opener.focus();window.opener.location.reload();window.opener = top;window.close();</script>";
		exit();
	}
}

$edit_mode=$_GET['edit']==1?True:False;
$chart_id=$_GET['chart_id'];
$sql = 'SELECT * FROM chart WHERE id=?';
$chart=	db_fetch_assoc(db_query($sql,array($chart_id)));
//var_dump($chart);

$view_id = $_GET['id'];
if (!test_access($view_id))
	die('access denied');
$view = db_fetch_assoc(db_query('SELECT * FROM view WHERE view.id=?',array($view_id)));
if (is_post()) {
	$_GET['date1'] = trim($_POST['date1']);
	$_GET['date2'] = trim($_POST['date2']);
	redirect($_SERVER['SCRIPT_NAME'].'?'.http_build_query($_GET));
}
$params = get_default_params(ors($view['day_range_default'], $default_steps));
$values = $keys = $charts = array();
$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $view['sql']);
$sth = db_query($sql, $params);
if ($row = db_fetch_assoc($sth)) {
	$keys = array_keys($row);
	$values[] = array_values($row);
	while ($row = db_fetch_array($sth)) {
		$values[] = $row;
	}
}
if ($keys) {
	$sql = 'SELECT * FROM chart WHERE view_id=? ORDER BY tab_order';
	$sth = db_query($sql,array($view_id));
	while ($row = db_fetch_assoc($sth)) {
		$legends = get_legends(array_flip($keys),explode(',',$row['columns']));
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
<title>Chart Setting</title>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>

<link type="text/css" href="css/multiselect/ui.multiselect.css" rel="stylesheet" />

<style type="text/css">
	#tabs { font-size:70%; }
	th,td { font-size: 8pt; }
	.main_table { border-collapse: collapse; background-color: white;  }
	.main_table td { white-space: nowrap; text-align:right; padding:3px; }
	.main_table tbody tr:nth-child(odd) { background-color: #EEE; } /* IE not support */
	.chart_title { text-align: right; padding-right: 5px; }
	.chart_block { margin-bottom: 10px; }
	.chart_descr { padding: 15px 10px; }
	.view_descr { padding: 15px 0px; }
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
		var tabs = $('#tabs').tabs({
			cookie: { expires: 30 }
		});
		//$.localise('ui-multiselect', {/*language: 'en',*/ path: 'js/locale/'});
		$(".multiselect").multiselect();
	});
</script>
</head>
<body>


<?
$options = json_decode(if_blank($chart['options'],'[]'),TRUE);
//var_dump($options);

$size=explode('x',$options['Template']['Size']);
?>


<form action='' method='post'>
<table width="400" border="0" cellpadding="3">
  <tr>
    <td width="100" align="right">View Title:</td>
    <td>
	<input name="action" type="hidden" value="save_chart_setting" />
	<input name="id" type="hidden" value="<?=$chart['id']?>" />
	<input name="chart_name" type="text" value="<?=$chart['name']?>" size="30" />
	</td>
  </tr>

  <tr>
    <td align="right">Chart Type:</td>
    <td>
	  <input type="radio" name="chart_type" value="1d1"<?=$options['Type']=='1d1'?' checked="checked"':''?> />1d1&nbsp;&nbsp;<img src="./refs/chart_1d1.png" width="100" height="50"/>&nbsp;&nbsp;

	  <input type="radio" name="chart_type" value="1d2"<?=$options['Type']=='1d2'?' checked="checked"':''?> />1d2&nbsp;&nbsp;<img src="./refs/chart_1d2.png" width="100" height="50"/>&nbsp;&nbsp;

	  <input type="radio" name="chart_type" value="1d3"<?=$options['Type']=='1d3'?' checked="checked"':''?> />1d3&nbsp;&nbsp;<img src="./refs/chart_1d3.png" width="100" height="50"/>&nbsp;&nbsp;

      <br/>

	  <input type="radio" name="chart_type" value="2d1"<?=$options['Type']=='2d1'?' checked="checked"':''?> />2d1&nbsp;&nbsp;<img src="./refs/chart_2d1.png" width="100" height="50"/>&nbsp;&nbsp;

	  <input type="radio" name="chart_type" value="2d2"<?=$options['Type']=='2d2'?' checked="checked"':''?> />2d2&nbsp;&nbsp;<img src="./refs/chart_2d2.png" width="100" height="50"/>&nbsp;&nbsp;

	  <input type="radio" name="chart_type" value="2d3"<?=$options['Type']=='2d3'?' checked="checked"':''?> />2d3&nbsp;&nbsp;<img src="./refs/chart_2d3.png" width="100" height="50"/>&nbsp;&nbsp;
    </td>
  </tr>
  <tr>
    <td align="right">Columns:</td>
    <td>
	  <select class="multiselect" multiple="multiple" name="chart_columns[]" style="width:500px;">

<?
if($chart['columns']){
$selectedCols=explode(',',$chart['columns']);
foreach ($selectedCols as $k2) {?>
        <option value="<?=$k2?>" selected="selected"><?=$k2?></option>
<?}}?>

<?foreach ($keys as $k2) {
	if(strpos($chart['columns'],$k2)===False){
	?>
        <option value="<?=$k2?>"><?=$k2?></option>
<?	}
}?>
      </select>
    </td>
  </tr>

  <tr>
    <td align="right">Aggregation:</td>
    <td>
	  <input type="radio" name="chart_aggregation" value=""<?=$options['Aggregation']==''?' checked="checked"':''?>/>None<br/>
	  <input type="radio" name="chart_aggregation" value="sum"<?=$options['Aggregation']=='sum'?' checked="checked"':''?>/>Use Sum of Values in Same Column<br/>
    </td>
  </tr>

  <tr>
    <td width="100" align="right">Max Shown Charts:</td>
    <td>
	<input name="chart_row_limit" type="text" value="<?=$options['RowLimit']?>" size="30" />
	</td>
  </tr>

  <tr>
    <td align="right">Adjust Range:</td>
    <td>
	  <input type="radio" name="chart_start_zero" value="0"<?=$options['Template']['Options']['scaleStart0']!='1'?' checked="checked"':''?>/>Auto Adjust<br/>
	  <input type="radio" name="chart_start_zero" value="1"<?=$options['Template']['Options']['scaleStart0']=='1'?' checked="checked"':''?>/>No Adjust<br/>
    </td>
  </tr>

  <tr>
    <td width="100" align="right">Chart Size:</td>
    <td>
	  Width <input name="chart_size_width" type="text" value="<?=$size[0]?>" size="6" />
	  Height <input name="chart_size_height" type="text" value="<?=$size[1]?>" size="6" />
	</td>
  </tr>

  <tr>
    <td width="100" align="right">Description:</td>
    <td><textarea name="chart_description" rows="5" cols="30" ><?=$chart['description']?></textarea></td>
  </tr>

  <tr>
    <td width="100" align="right">&nbsp;</td>
    <td><input type="submit" value="Save Setting" />
		<div style="float:right; margin:6px;"><a href="#" onclick="if(confirm('Are you sure to delete?'))location=('chart_setting.php?chart_id=<?=$chart['id']?>&action=delete');return false;" target="_blank" style="color:gray;text-decoration:none;">delete chart</a>
	</div>
</td>
  </tr>

</table>
</form>



</body>
</html>