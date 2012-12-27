<?php

function get_md5_key($oem_name,$category,$key){
	return '0x'.md5($oem_name.'|'.$category.'|'.$key);
}

function get_md5_sub_key($sub_key){
	return '0x'.md5($sub_key);
}

#echo get_md5_key('Umniah','moagent','app_page_by_url_pattern_daily_internal_total_process_time_average');
#echo get_md5_sub_key('link...');

function replace_get_md5_key($sql) {

	$pattern='/get_md5_key\(\s*[\'\"][^\'\"]*?[\'\"]\s*,\s*[\'\"][^\'\"]*?[\'\"]\s*,\s*[\'\"][^\'\"]*?[\'\"]\s*\)/i';
	preg_match_all ( $pattern, $sql , $matches);

	if($matches && $matches[0]){
		foreach($matches[0] as $v){
			eval('$get_md5_result='.$v.';');
			$sql=str_replace($v,$get_md5_result,$sql);
		}
	}

	return $sql;
}

function replace_get_md5_sub_key($sql) {

	$pattern='/get_md5_sub_key\(\s*[\'\"][^\'\"]*?[\'\"]\s*\)/i';
	preg_match_all ( $pattern, $sql , $matches);

	if($matches && $matches[0]){
		foreach($matches[0] as $v){
			eval('$get_md5_result='.$v.';');
			$sql=str_replace($v,$get_md5_result,$sql);
		}
	}

	return $sql;
}

#echo replace_get_md5('');

?>