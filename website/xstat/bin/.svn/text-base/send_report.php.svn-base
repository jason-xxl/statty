<?php
require('common.inc.php');
require_once('xstat.inc.php');
connect_db();

require(LIB_PCHART);
require_once('mail.inc.php');

function aggregate_views($user_id, $virtual_views) {
	$my_views = get_my_views($user_id);

	$template = array("Size" => "400x150");
	$views = array();
	foreach ($virtual_views as $view) {
		echo 1.5;
		print_r($view);
		$params = get_default_params(!empty($view['day_range_default']) ? '-'.$view['day_range_default'].' days' : '-1 month');
		$values = $keys = $view['cols'] = array();
		$sth = db_query('SELECT `sql`,view_id,col_name,alias FROM virtual_view_item, view WHERE view_id=view.id AND virtual_view_id=? ORDER BY seq',array($view['id']));
		for ($i = 0; $row = db_fetch_assoc($sth); $i++) {
			if ($my_views[$row['view_id']])
				$keys[$row['sql']][$i] = $row['col_name'];
			$view['cols'][] = array('name'=>ors($row['alias'],$row['col_name']));
			print_r( array('name'=>ors($row['alias'],$row['col_name'])));
		}
		foreach ($keys as $sql => $names) {
			$sql = str_replace('%where_and_more%', ' AND date BETWEEN :date1 AND :date2', $sql);
			echo $sql;
			print_r($params);
			$sth = db_query($sql, $params);
			for ($i = 0; $row = db_fetch_assoc($sth); $i++) {
				echo 1.6;
				print_r($row);

				list($k,$v) = each($row);
				$values[$v][0] = $v;
				foreach ($names as $j => $name)
					$values[$v][$j+1] = $row[$name];
			}
		}
		echo 1.75;print_r($values);

		if (!$view['cols'] || !$values) continue;
		krsort($values);
		foreach(array_keys($values) as $i => $k) {
			foreach ($values[$k] as $j => $v) {
				if ($j == 0) continue;
				$aggr =& $view['cols'][$j-1];
				if ($i == 0) {
					$d1 = strtotime($k);
					$aggr['last'] = $aggr['max'] = $aggr['min'] = $v;
				} else {
					$aggr['max'] = max($aggr['max'], $v);
					$aggr['min'] = min($aggr['min'], $v);
				}
				$d = ($d1-strtotime($k))/SECONDS_PER_DAY;
				if ($d < 7)
					$aggr['last_avg_w'] = average($aggr['last_avg_w'], $v);
				if ($d < 30)
					$aggr['last_avg_m'] = average($aggr['last_avg_m'], $v);
				if (($w = (int)($d/7)) < 3)
					$aggr['last_avg_3w'][$w] = average($aggr['last_avg_3w'][$w], $v);
			}
		}
		foreach ($view['cols'] as $i => &$_c)
			$_c['pchart'] = to_2d_chart_data($values, '2d4', array($_c['name'] => $i+1), $template);
				
		echo 2;
		print_r($view);

		$views[] = $view;
	}
		echo 2.5;
		print_r($views);

	return $views;
}
ob_start();
$user_views = db_query("SELECT user_id,id,day_range_default,name,email_cc FROM virtual_view WHERE enabled>0 AND subscribed>0 and user_id=35")
		->fetchAll(PDO::FETCH_ASSOC|PDO::FETCH_GROUP);

foreach ($user_views as $user_id => $virtual_views) {
	$images = array();
	@$email = explode(",",$virtual_views[0]['email_cc']);
	$email[] = db_query_for_one('SELECT email FROM user WHERE id=?',array($user_id));
	if(empty($email)) {
		continue;
	}
	//if (!($email = db_query_for_one('SELECT email FROM user WHERE id=?',array($user_id))))
	//	continue;
?>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GBK">
<style type="text/css">
	body {font-family: Arial, Helvetica, sans-serif; font-size: 10.5pt; }
	body>table {border-collapse: collapse; }
	caption { text-align: left; }

</style>
</head>
<body>

<?
	
foreach (aggregate_views($user_id, $virtual_views) as $view) {?>
<table>
<tr><th><h3><?=$view['name']?></h3></th></tr>
<?foreach ($view['cols'] as $i => $col) { $images[] = PCACHE_PATH.'/'.$col['pchart'];?>
<tr><td><table width="100%">
<caption align="left"><strong><?=q($col['name'])?></strong>: <?=n($col['last'])?></caption>
<tr><td>
<img src="<?=hash('crc32',$col['pchart'])?>.png"/>
</td>
<td valign="bottom">
<div style="line-height: 20px">1 week avg: <?=n(e($col['last_avg_w']))?></div>
<div style="line-height: 20px">1 month avg: <?=n(e($col['last_avg_m']))?></div>
</td></tr>
</table></td></tr>
<?}?>
</table>
<hr/>
<br/>
<?}?>
</body>
</html>
<?
	
	sendmail('admin@mozat.com', $email, 'Mozat Stats Report ['.date('Y-m-d',time()-SECONDS_PER_DAY).']', 
		ob_get_contents(), array(), array(), $images);
	ob_clean();
	
}
?>