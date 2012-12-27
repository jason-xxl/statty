<?php
set_time_limit(180);
require('common.inc.php');
require(LIB_PCHART);
page_start();

$charts = array();
$types = explode(',','1d1,1d2,1d3,2d1,2d2,2d3,2d4,2d5');
$chart = array(
	//'Size'=>'500x182'
);
$row = array(
	'columns'=>'Apple,Orange,Papaya',
);
$keys = explode(',','Date,Orange,Apple,Papaya,Banana');
$values = array(
	explode(',','2010-06-08,1000,1356,1456,1236'),
	explode(',','2010-06-07,1568,1626,1246,1789'),
	explode(',','2010-06-06,1234,1842,1895,3245'),
	explode(',','2010-06-05,1469,2156,2136,1589'),
);
$legends = get_legends(array_flip($keys),explode(',',$row['columns']));
foreach ($types as $t) {
	if (starts_with($t, '1d'))
		$charts[$t] = to_1d_chart_data($values[0], $t, $legends, $chart);
	if (starts_with($t, '2d'))
		$charts[$t] = to_2d_chart_data(array_slice($values,0,10), $t, $legends, $chart);
}
?>
<html>
<head>
<style type="text/css">
	i { color: blue; }
</style>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
</head>
<body>

<div><b>Virtual View Option Sample:</b></div>
<pre style="border: 1px solid gray; background-color: #F0F0F0">
{
non_time_key: false
}
</pre>

<div><b>Page Column Option Sample:</b></div>
<pre style="border: 1px solid gray; background-color: #F0F0F0">
{
"Type": "2d1", <i>(default: 2d4)</i>
"Template": {
		"DataDescription": {
			"Format": {"Y":"metric"}
		},
		"Options": {"scaleStart0":1},
		"Font": 8,
		"Size": "500x200"
	},
"div_style": "width: 500px", <i>(div container style)</i>
"avg_loc": "bottom", <i>(options: bottom, right, [empty])</i>
"link_b": "view.php?id=1", <i>(after average at bottom)</i>
"link_r": "view.php?id=1", <i>(after average at right)</i>
"span": 3, <i>(composite chart by 3 columns)</i>
"span_order": "desc", <i>(options: desc, asc, [empty])</i>
"break": "&lt;hr/&gt;", <i>(at most top, outside div container)</i>
"after": "&lt;div&gt;hello&lt;/div&gt;", <i>(at most bottom, outside div container)</i>
"header": "&lt;hr/&gt;", <i>(header at top, inside div container)</i>
"title": "PV Compare", <i>(Chart title, default is column name)</i>
"descr": "this is a test" <i>(description at bottomm)</i>
}

Note: 1d chart auto aggreation sum
</pre>


<div><b>Chart Option Sample:</b></div>
<pre style="border: 1px solid gray; background-color: #F0F0F0">
{
"Type": "2d1",
"RowLimit": 10,
"Aggregation": "sum",
"Template": {
		"DataDescription": {
			"Format": {"Y":"metric"}
		},
		"Options": {"scaleStart0":1},
		"Font": 8,
		"Size": "500x200"
	}
}
</pre>
<?foreach ($charts as $t => $h) {?>
<span>
<div><?=$t?></div>
<img src="chart.php?h=<?=$h?>"/>
</span>
<?}?>

</body>
</html>