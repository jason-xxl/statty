<?php
function sec_to_date($val) {
	return date('Y-m-d', $val);
}

//echo sec_to_date(1263603510.85);

function sec_slice_to_days($val) {
	return number_format($val / 3600 / 24, 2) . ' day(s)';
}

//echo sec_slice_to_days(5942548.07);

function na($val) {
	return 'n/a';
}

function profile_birthday_to_text($v) {
	return sec_to_date($v);
}

function profile_gender_to_text($v) {
	return 'Male: '.number_format((2-$v)*100,2).'% , Female: '.number_format(100-(2-$v)*100,2).'%';
}

function subscribe_in_subscription_to_text($v) {
	return 'Sub: '.number_format(($v)*100,2).'% , Unsub: '.number_format((1-$v)*100,2).'%';
}

function profile_last_login_time_to_text($v) {
	return sec_to_date($v);
}

function subscribe_last_unsubscribe_time_to_text($v) {
	return sec_to_date($v);
}

function subscribe_first_subscribe_reg_time_to_text($v) {
	return sec_to_date($v);
}

function subscribe_max_sub_duration_to_text($v) {
	return sec_slice_to_days($v);
}

function subscribe_msisdn_to_text($v) {
	return na($v);
}

function default_to_text($v) {
	if (is_numeric($v) && strpos($v, '.') !== False) {
		return number_format($v, 2);
	}
	return $v;
}

function data_to_text($v,$col_name){
	$func_name=str_replace('-','_',$col_name).'_to_text';
	if(!function_exists($func_name)){
		return default_to_text($v);
	}
	return call_user_func($func_name, $v);
}


?>