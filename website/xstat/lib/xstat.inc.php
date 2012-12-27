<?php
function _cmp_view_name($a, $b = NULL) {
	 $index = array_flip(array('Puzzle_Trooper','','STC','Shabik_360',/*'Zoota',*/'Globe','UMobile','AIS',/*'Loops',*/'Umniah','Viva','Viva_BH','Telk_Armor','Vodafone','Mozat','LOOPsChat','Operators_Reports','Report','Technical','Project','Privileged','Test_Umniah','Unsupported','DI'));
	 if (is_null($b))
	 	return $index[substring_before($a, ' ')];
	 return strcmp(preg_replace('/^(\w+)\b/e','sprintf("%02s \1",$index["\1"])', $a),
	 	preg_replace('/^(\w+)\b/e','sprintf("%02s \1",$index["\1"])', $b));
}
function get_my_views($user_id, $no_sort = FALSE) {
	$sql = 'SELECT id,name FROM view order by name asc';
	//if (!is_admin($user_id))
	//	$sql .= ' WHERE id in (SELECT DISTINCT view_id FROM group_to_view JOIN user_to_group USING (group_id) WHERE user_id=:user_id)';
	$views = db_query_for_assoc($sql, array('user_id'=>$user_id));
	//echo $sql;
	//var_dump($views);
	//echo $user_id;
	//if (!$no_sort) {
	//	$views = array_filter($views, '_cmp_view_name');
	//	uasort($views, '_cmp_view_name');
	//}
	//var_dump($user_id);
	return $views;
}
function test_access($view_id) {
	$views = get_my_views(sess_get_uid(), TRUE);
	return $views[$view_id];
}
function is_admin($user_id = NULL) {
	//echo 'uid_'.$user_id;
	return true; 
	if (empty($user_id) && sess_get_uid())
		return in_array(1, $_SESSION['U']['GROUPS']);
	return $user_id && db_query_for_one("SELECT COUNT(*) FROM user_to_group WHERE user_id=? AND group_id=1", array($user_id)) > 0;
}
function get_view_script($view_id) {
	$views = array();
	if (in_array($view_id, $views))
		return 'view2.php';
	return 'view.php';
}
function get_steps() {
	if ($_GET['group_by'] == 'weekly')
		return 7;
	elseif ($_GET['group_by'] == 'monthly')
		return 30;
	elseif ($_GET['group_by'] == 'seanonly')
		return 364/4;
	elseif ($_GET['group_by'] == 'yearly')
		return 365;
	return 1;
}
function get_default_params($default_range = 0,$include_today=0,$default_start_date='') {
	if ($default_range) {
		$step_name = if_blank(preg_replace('/^(week|month|year)ly$/','\1s',$_GET['group_by']), 'days');
		if ($step_name != 'days')
			$default_range = "-$default_range $step_name +1 day";
		else
			$default_range = $default_range==1 ? 'today' : (1-$default_range)." days";

		$_GET['date2'] = date('Y-m-d',$date2 = strtotime(if_blank($_GET['date2'],$include_today?'today':'yesterday')));

		if ($default_start_date && empty($_GET['date1'])) {
			$_GET['date1'] = $default_start_date;
		} else {
			$_GET['date1'] = date('Y-m-d',strtotime(if_blank($_GET['date1'],$default_range),$date2));
		}
	}

	$dates_int=array();
	$dates_str=array();
	for($i=strtotime($_GET['date1']);$i<=strtotime($_GET['date2']);$i+=3600*24){
		$dates_int[]=strftime('%Y%m%d',$i);
		$dates_str[]='\''.strftime('%Y-%m-%d',$i).'\'';
	}
	$dates_int_str=implode(',',$dates_int);
	$dates_str_str=implode(',',$dates_str);
	if(empty($dates_int_str)){
		$dates_int_str='0';
		$dates_str_str='\''.'unknown'.'\'';
	}
	#echo $dates_str_str;
	#exit();
	$ret=array(
		'dates_int_str'=>$dates_int_str,
		'dates_str_str'=>$dates_str_str,
		'date2' => $_GET['date2'],
		'date1' => $_GET['date1'],
		'base_date_1' => if_blank($_GET['base_date_1'],date('Y-m-d',strtotime('-2 days'))),
		'base_date_2' => if_blank($_GET['base_date_2'],date('Y-m-d',strtotime('-7 days'))),
		'target_date' => if_blank($_GET['target_date'],date('Y-m-d',strtotime('-1 days'))),
		'level' => if_blank($_GET['level'],'0.75'),

	);

	for($i=100;$i>=1;$i--){
		$ret['key'.$i]=if_blank($_GET['key'.$i],'');
	}
	return $ret;
}

function average($avg, $val) {
	if (!$val)
		return NULL;
	$v = $avg ? explode('/', $avg) : array(0, 0);
	return sprintf('%s/%d', $v[0] + $val, $v[1] + 1);
}
function dir_of_array($val) {
	$d = $l = NULL;
	foreach($val as $v) {
		$v = e($v);
		if (is_null($l))
			$l = $v;
		elseif (is_null($d))
			$d = sign($v-$l);
		elseif ($d != 0 && $d !=sign($v-$l))
			$d = 0;
	}
	return ors($d, 0);
}
function aggregate_col(&$col, $v, $d, $t) {
	if ($d < 0)
		return;
	if ($d == 0) {
		$col['last'] = $v;
		$col['last_dt'] = $t;
	}
	if (is_null($col['max'])) {
		$col['max'] = $col['min'] = $v;
	} else {
		$col['max'] = max($col['max'], $v);
		$col['min'] = min($col['min'], $v);
	}
	if ($d < 7)
		$col['last_avg_w'] = average($col['last_avg_w'], $v);
	if ($d < 30)
		$col['last_avg_m'] = average($col['last_avg_m'], $v);
	if (($w = (int)($d/7)) < 3)
		$col['last_avg_3w'][$w] = average($col['last_avg_3w'][$w], $v);
}



function getAggregatingSql($originalViewSql,$params,$agg_unit,$view_id){

	switch($agg_unit){
		case 'weekly':
			if(!empty($view_id)){
				$agg_sql='select `sql` from `view_weekly` where `id`='.$view_id;
				$agg_sql=db_query($agg_sql, null);
				$agg_sql=db_fetch_assoc($agg_sql);
				if(!empty($agg_sql['sql'])){
					return $agg_sql['sql'];
				}
			}
			$aggColTpl='yearweek(`auto_aggr_source`.`{time_col}`,5)';
			$timeColTpl='max(`auto_aggr_source`.`{time_col}`) as `{time_col}`';

			$filterIncompletePeriodSqlTpl='select * from ({aggregated_sql}) `aggregated_result` where weekday(`aggregated_result`.`{time_col}`)=6';
			break;
		case 'monthly':
			if(!empty($view_id)){
				$agg_sql='select `sql` from `view_monthly` where `id`='.$view_id;
				$agg_sql=db_query($agg_sql, null);
				$agg_sql=db_fetch_assoc($agg_sql);
				if(!empty($agg_sql['sql'])){
					return $agg_sql['sql'];
				}
			}
			$aggColTpl='date_format(`auto_aggr_source`.`{time_col}`,"%Y-%m-00")';
			$timeColTpl='max(`auto_aggr_source`.`{time_col}`) as `{time_col}`';
			$filterIncompletePeriodSqlTpl='select * from ({aggregated_sql}) `aggregated_result` where DAYOFMONTH(date_add(`aggregated_result`.`{time_col}`,interval 1 day))=1';
			break;
		case 'seasonly':
			if(!empty($view_id)){
				$agg_sql='select `sql` from `view_seasonly` where `id`='.$view_id;
				$agg_sql=db_query($agg_sql, null);
				$agg_sql=db_fetch_assoc($agg_sql);
				if(!empty($agg_sql['sql'])){
					return $agg_sql['sql'];
				}
			}
			$aggColTpl='concat(year(`auto_aggr_source`.`{time_col}`),"-0",floor((month(`auto_aggr_source`.`{time_col}`)-1)/3))';
			$timeColTpl='max(`auto_aggr_source`.`{time_col}`) as `{time_col}`';
			$filterIncompletePeriodSqlTpl='';
			break;
		case 'yearly':
			if(!empty($view_id)){
				$agg_sql='select `sql` from `view_yearly` where `id`='.$view_id;
				$agg_sql=db_query($agg_sql, null);
				$agg_sql=db_fetch_assoc($agg_sql);
				if(!empty($agg_sql['sql'])){
					return $agg_sql['sql'];
				}
			}
			$aggColTpl='concat(year(`auto_aggr_source`.`{time_col}`),"-00-00")';
			$timeColTpl='max(`auto_aggr_source`.`{time_col}`) as `{time_col}`';
			$filterIncompletePeriodSqlTpl='';
			break;
		default:
			return $originalViewSql;
			break;
	}



	$sql = str_replace('%where_and_more%', ' AND 0', $originalViewSql);
	
	$sth = db_query($sql, $params);
	$row = db_fetch_assoc($sth);

	$colCount=$sth->columnCount();
	$colMeta=$sth->getColumnMeta(0);
	$oldIndexCol=$colMeta['name'];

	if(strtolower($oldIndexCol) != 'time' && strtolower($oldIndexCol) != 'date'){
		return $originalViewSql;
	}

	$aggCol=str_replace('{time_col}',$oldIndexCol,$aggColTpl);
	$timeCol=str_replace('{time_col}',$oldIndexCol,$timeColTpl);
	$filterIncompletePeriodSqlTpl=str_replace('{time_col}',$oldIndexCol,$filterIncompletePeriodSqlTpl);

	$aggregatingSql='select ';
	$aggregatingSql .= $timeCol;

	for($i=1;$i<$colCount;$i++){
		$colMeta=$sth->getColumnMeta($i);

		switch($colMeta['native_type']){
			case 'DOUBLE':
				$aggregatingSql.=', sum(`auto_aggr_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				break;
			case 'LONG_BLOB':
				
				if(preg_match('/\brate$/i',$colMeta['name'])>0){
					$aggregatingSql.=', concat(format(avg(replace(`auto_aggr_source`.`'.$colMeta['name'].'`,"%","")),2),"%") as `'.$colMeta['name'].'`';
				}else{
					$aggregatingSql.=', max(`auto_aggr_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				}
				
				break;

			default:
				$aggregatingSql.=', sum(`auto_aggr_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				break;			
		}
		//var_dump($colMeta);
	}

	$originalViewSql=preg_replace('/;+[\s\n]*$/','',$originalViewSql);

	$aggregatingSql.=' from ('.$originalViewSql.') `auto_aggr_source`'
	.' group by '.$aggCol
	.' order by '.$aggCol.' desc';
	
	if(!empty($filterIncompletePeriodSqlTpl)){
		$aggregatingSql=str_replace('{aggregated_sql}',$aggregatingSql,$filterIncompletePeriodSqlTpl);
		//echo $aggregatingSql;
	}
	
	//map weekly/monthly uv columns to related name space

	switch($agg_unit){
		case 'weekly':
			$aggregatingSql=preg_replace("#(`?\bkey\b`?\s*=\s*['\"])([^'\"]*?)(_unique['\"])#","$1$2_weekly$3",$aggregatingSql);
			break;
		case 'monthly':
			$aggregatingSql=preg_replace("#(`?\bkey\b`?\s*=\s*['\"])([^'\"]*?)(_unique['\"])#","$1$2_monthly$3",$aggregatingSql);
			break;
		case 'seasonly':
			$aggregatingSql=preg_replace("#(`?\bkey\b`?\s*=\s*['\"])([^'\"]*?)(_unique['\"])#","$1$2_seasonly$3",$aggregatingSql);
			break;
		case 'yearly':
			$aggregatingSql=preg_replace("#(`?\bkey\b`?\s*=\s*['\"])([^'\"]*?)(_unique['\"])#","$1$2_yearly$3",$aggregatingSql);
			break;
		default:
			break;
	}

	return $aggregatingSql;
}



function getMaxSql($originalViewSql,$params){

	$sql = str_replace('%where_and_more%', ' AND 0', $originalViewSql);
	$sth = db_query($sql, $params);
	$row = db_fetch_assoc($sth);

	$colCount=$sth->columnCount();
	$colMeta=$sth->getColumnMeta(0);
	$oldIndexCol=$colMeta['name'];

	if(strtolower($oldIndexCol) != 'time' && strtolower($oldIndexCol) != 'date'){
		return $originalViewSql;
	}

	$maxSql='select max(`'.$oldIndexCol.'`) as `'.$oldIndexCol.'`' ;

	for($i=1;$i<$colCount;$i++){
		$colMeta=$sth->getColumnMeta($i);

		switch($colMeta['native_type']){

			case 'LONG_BLOB':

				if(preg_match('/\brate$/i',$colMeta['name'])>0){
					$maxSql.=', concat(format(max(replace(`auto_stat_max_source`.`'.$colMeta['name'].'`,"%","")),2),"%") as `'.$colMeta['name'].'`';
				}else{
					$maxSql.=', max(`auto_stat_max_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				}
				
				break;

			default:
				$maxSql.=', max(`auto_stat_max_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				break;			
		}
		//var_dump($colMeta);
	}


	$originalViewSql=preg_replace('/;+[\s\n]*$/','',$originalViewSql);

	$maxSql.=' from ('.$originalViewSql.') `auto_stat_max_source`';

	return $maxSql;
}



function getAvgSql($originalViewSql,$params){

	$sql = str_replace('%where_and_more%', ' AND 0', $originalViewSql);
	$sth = db_query($sql, $params);
	$row = db_fetch_assoc($sth);

	$colCount=$sth->columnCount();
	$colMeta=$sth->getColumnMeta(0);
	$oldIndexCol=$colMeta['name'];

	if(strtolower($oldIndexCol) != 'time' && strtolower($oldIndexCol) != 'date'){
		return $originalViewSql;
	}

	$maxSql='select max(`'.$oldIndexCol.'`) as `'.$oldIndexCol.'`' ;

	for($i=1;$i<$colCount;$i++){
		$colMeta=$sth->getColumnMeta($i);

		switch($colMeta['native_type']){

			case 'LONG_BLOB':

				if(preg_match('/\brate$/i',$colMeta['name'])>0){
					$maxSql.=', concat(format(avg(replace(`auto_stat_avg_source`.`'.$colMeta['name'].'`,"%","")),2),"%") as `'.$colMeta['name'].'`';
				}else{
					$maxSql.=', max(`auto_stat_avg_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				}
				
				break;

			default:
				$maxSql.=', avg(`auto_stat_avg_source`.`'.$colMeta['name'].'`) as `'.$colMeta['name'].'`';
				break;			
		}
		//var_dump($colMeta);
	}


	$originalViewSql=preg_replace('/;+[\s\n]*$/','',$originalViewSql);

	$maxSql.=' from ('.$originalViewSql.') `auto_stat_avg_source`';

	return $maxSql;
}


?>
