<?php
function output_pchart(&$_PAGE, $index) {
	global $view, $values, $values_sum, $pChart;
	$col =& $view['cols'][$index];
	$legends = array();
	foreach (range($index, $index + ors($col['span'], 1)-1) as $i)
		$legends[$view['cols'][$i]['name']] = $i + 1;
	if ($col['span_order'] && $col['span'] > 1) {
		$map = array();
		foreach (range($index, $index + ors($col['span'], 1)-1) as $i)
			$legends[$view['cols'][$i]['name']] = $values_sum[$map[$view['cols'][$i]['name']] = $i + 1];
		if ($col['span_order'] == 'desc')
			arsort($legends);
		elseif ($col['span_order'] == 'asc')
			asort($legends);
		foreach ($legends as $n => &$_v)
			$_v = $map[$n];
	} else {
		foreach (range($index, $index + ors($col['span'], 1)-1) as $i)
			$legends[$view['cols'][$i]['name']] = $i + 1;
	}
	$type = ors($col['Type'], '2d4');
	$template = ors($col['Template'], array());
	if (starts_with($type, '1d'))
		$col['chart'] = to_1d_chart_data($values_sum, $type, $legends, $template);
	elseif (starts_with($type, '2d'))
		$col['chart'] = to_2d_chart_data($values, $type, $legends, $template);
?>
<img src="chart.php?h=<?=$col['chart']?>"<?=$pChart['ImgSize'][3]?>"/>
<?
}
function output_subpage_pchart(&$_PAGE, $index) {
	global $view, $min_width, $yesterday;
	$type = ors($view['cols'][$index]['Type'], '2d4');
	$span = ors($view['cols'][$index]['span'], 1);
	if (($date2 = strtotime($_GET['date2'])) >= $yesterday)
		$attr_zoom_out_right = ' disabled="disabled"';
	if (($date2 - strtotime($view['cols'][$index]['first_dt']))/SECONDS_PER_DAY + 1 < $min_width)
		$attr_zoom_in_left = $attr_zoom_in_right = ' disabled="disabled"';
?>
<form action="<?=$_PAGE['.']?>" method="post">
<? output_pchart($_PAGE, $index); ?>
<?if (starts_with($type, '2d') && !$view['opt']['non_time_key']) {?>
<div class="navi_bar">
<span style="float: left;">
<button type="submit" class="img_btn {post:{action:'zoom-out-left'}}" title="Zoom Out"><img src="images/x-zoom-out.png" class="grayscaleable" width="24" class="hover"/></button>
<button type="submit" class="img_btn {post:{action:'zoom-in-left'}} zoom-in"<?=$attr_zoom_in_left?> title="Zoom In"><img src="images/x-zoom-in.png" class="grayscaleable" width="24" class="hover"/></button>
</span>
<span style="float: right;">
<button type="submit" class="img_btn {post:{action:'zoom-in-right'}} zoom-in"<?=$attr_zoom_in_right?> title="Zoom In"><img src="images/x-zoom-in.png" class="grayscaleable" width="24" class="hover"/></button>
<button type="submit" class="img_btn {post:{action:'zoom-out-right'}}"<?=$attr_zoom_out_right?> title="Zoom Out"><img src="images/x-zoom-out.png" class="grayscaleable" width="24" class="hover"/></button>
</span>
<input type="hidden" name="pagegroup" value="pchart"/>
<input type="hidden" name="page" value="<?=$index?>"/>
<input type="hidden" name="span" value="<?=$span?>"/>
<input type="hidden" name="date1" value="<?=$_GET['group_by']?$view['cols'][$index]['first_dt']:$_GET['date1']?>"/>
<input type="hidden" name="date2" value="<?=$_GET['date2']?>"/>
</div>
<div class="overlay"></div>
<?}?>
</form>
<?
}
?>