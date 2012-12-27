<?php
set_time_limit(180);
require('common.inc.php');
page_start();
if (!sess_get_uid()) {
	if (is_post())
		stop('Session timeout!');
	redirect('login.php');
}
require('xstat.inc.php');
$_PAGE['view_id'] = $_GET['id'];
if (!db_query_for_one('SELECT COUNT(*) FROM virtual_view WHERE id=? AND user_id=?',array($_PAGE['view_id'],sess_get_uid())))
	stop('Access denied!');
$views = get_my_views(sess_get_uid());

include('subpages/edit_view_v.php');
if (is_post()) {
	if ($_POST['page'] == 'view') {
		if ($_POST['action'] == 'update') {
			try {
				db_update('UPDATE virtual_view SET name=?, description=?, day_range_default=? WHERE id=?',
					array(trim($_POST['name']), trim($_POST['description']), trim($_POST['day_range_default']), $_PAGE['view_id']));
				$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Update saved!');
			} catch (PDOException $err) {
				if ($err->errorInfo[1] == 1062)
					$_PAGE['page_msg'][] = array('type'=> 'error','text'=>'Updating view failed: dupliadte name');
			}
		}
		output_subpage_view($_PAGE);
		exit(0);
	} elseif ($_POST['page'] == 'cols') {
		if ($_POST['action'] == 'reset')
			unset($_POST['view']);
		elseif ($_POST['action'] == 'add_col' && $views[$_POST['view_id']]) {
			try {
				$seq = db_query_for_one('SELECT MAX(seq) FROM virtual_view_item WHERE virtual_view_id=?', array($_PAGE['view_id']));
				db_update('INSERT INTO virtual_view_item (virtual_view_id,col_name,alias,view_id,seq) VALUE (?,?,?,?,?)', array($_PAGE['view_id'], $_POST['col'], $_POST['alias'], $_POST['view_id'],$seq+1));
				if (!$seq)
					db_update('UPDATE virtual_view SET enabled=1 WHERE id=? AND enabled=0', array($_PAGE['view_id']));
				$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Column added!');
			} catch (PDOException $err) {
				$_PAGE['page_msg'][] = array('type'=> 'error','text'=>'Adding column failed');
			}
		} elseif ($_POST['action'] == 'delete') {
			try {
				db_update('DELETE FROM virtual_view_item WHERE id=? AND virtual_view_id=?', array($_POST['col_id'], $_PAGE['view_id']));
				db_update('UPDATE virtual_view SET enabled=0 WHERE id=? AND NOT EXISTS (SELECT id FROM virtual_view_item WHERE virtual_view_id=virtual_view.id)', array($_PAGE['view_id']));
				$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Column removed!');
			} catch (PDOException $err) {
			}
		} elseif ($_POST['move']) {
			$o = split(',',$_POST['move']);
			if ($o[0]>=$o[1] || $o[2] && $o[1]>=$o[2]) {
				db_update('UPDATE virtual_view_item SET seq=IF(seq=:o1,:o0,seq)+1 WHERE virtual_view_id=:id AND (:o2>0 AND :o2-:o0<=1 AND seq>=:o2 OR seq=:o1)',array('o0'=>$o[0],'o1'=>$o[1],'o2'=>$o[2],'id'=>$_PAGE['view_id']));
				$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Column moved!');
			}
		}
		output_subpage_cols($_PAGE);
		exit(0);
	} elseif ($_POST['page'] == 'chart') {
		output_subpage_chart($_PAGE);
		exit(0);
	} elseif ($_POST['page'] == 'charts') {
		if ($_POST['action'] == 'add') {
			db_query("INSERT INTO virtual_view_chart (name,description,columns,options,view_id) values(?,?,?,?,?)",
				array($_POST['name'],$_POST['description'],join(',',$_POST['columns']),json_encode(get_chart_options()),$_PAGE['view_id']));
			$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Chart created!');
		} elseif ($_POST['action'] == 'update') {
			db_query("UPDATE virtual_view_chart SET name=?,description=?,columns=?,options=? WHERE view_id=? AND id=?",
				array($_POST['name'],$_POST['description'],join(',',$_POST['columns']),json_encode(get_chart_options()),$_PAGE['view_id'],$_POST['chart_id']));
			$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Chart updated!');
		} elseif ($_POST['action'] == 'delete') {
			db_update('DELETE FROM virtual_view_chart WHERE id=? AND view_id=?', array($_POST['chart_id'], $_PAGE['view_id']));
			$_PAGE['page_msg'][] = array('type'=> 'ok','text'=>'Chart removed!');
		}
		output_subpage_charts($_PAGE);
		exit(0);
	}
}
function get_chart_options() {
	return array(
		"Type"=> ors($_POST['type'],'1d1'),
		"RowLimit"=> ors($_POST['row_limit'],0),
		"Aggregation"=> ors($_POST['aggregation'],''),
		"Template"=>array(
			"DataDescription"=>array( "Format"=>array("Y"=>"metric") ),
			"Options"=>array("scaleStart0"=>$_POST['start_zero']==='0'?0:1),
			"Font"=>8,
			"Size"=>ors($_POST['size_width'],'1000').'x'.ors($_POST['size_height'],'400')
		)
	);
}
?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<link href="common.css" rel="stylesheet" type="text/css"/>
<style type="text/css">
body { width: 800px; }
fieldset { margin-bottom: 2px; }
.ui-dialog { font-size: 100%!important; }
.onDrag { background-color: #BBBBFF; }
.onDrag img.dragHandle { opacity: 1; }
ul.ui-autocomplete { max-height: 250px; overflow-y: auto; }
img.dragHandle { cursor: move; padding: 1px 6px; opacity: .3; }
img.dragHandle:hover { opacity: 1; }
</style>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" src="js/jquery.metadata.js"></script>
<script type="text/javascript" src="js/jquery.validate.js"></script>
<script type="text/javascript" src="js/jquery.form.js"></script>
<script type="text/javascript" src="js/jquery.tablednd_0_5.js"></script>
<script type="text/javascript" src="js/jquery.blockUI.js"></script>
<script type="text/javascript" src="js/jquery.dd.js"></script>
<link href="css/dd.css" rel="stylesheet" type="text/css" />
<script type="text/javascript" src="js/ui.multiselect.js"></script>
<link href="css/ui.multiselect.css" type="text/css" rel="stylesheet" />
<script type="text/javascript" src="common.js"></script>
<script type="text/javascript" charset="utf-8">
function on_load() {
	if ($('.page_msg > *',this).size()  > 0)
		$('.page_msg',this).fadeIn('slow').delay(2000).fadeOut('slow');
	if (!$(this).is('.ui-dialog-content'))
		$('.ui-dialog-content').dialog('close');
	$('#sel_view',this).autocomplete({
		source: <?=json_encode(array_values($views))?>,
		close: function(event, ui) {
			$(this).blur().parent().block({message:'<img src="images/ajax-loader3.gif" align="center"/>'});
			$(this.form).data('options',{noblock:true}).data('srcElement',this).submit();
		}
	}).focus(function() {
		if ($(this).hasClass("to_search"))
			$(this).removeClass("to_search");
	}).blur(function() {
	    if ($.trim(this.value) == '')
	    	$(this).addClass("to_search");
	}).each(function() {
	    if ($.trim(this.value) == '')
	    	$(this).addClass("to_search");
	});
	$('#tbl-cols',this).tableDnD({
		onDragClass: 'onDrag',
		dragHandle: "dragHandle",
		onDrop: function(table, row) {
			if ( parseInt($('td:first-child',$(row).prev()).text()||'1')+1 != $('td:first-child',row).text()) {
				var form = $(table).closest('form');
				var v = ($(row).prev().attr('seq')||'0')+','+$(row).attr('seq')+','+($(row).next().attr('seq')||'0');
				$(row).addClass('onDrag');
				form.data('options', {post:{move:v}}).submit();
			}
		}
	});
	$('.imageselect',this).msDropDown({mainCSS:'dd'});
	$('.multiselect',this).multiselect();
}
function reset_view(btn) {
	$('#sel_view').removeAttr('readonly').val('').addClass('to_search');
	$(btn).closest('div').parent().find('div:has(#sel_view) ~ div').remove();
	$(btn).remove();
}
function open_modal(title, width, height) {
	var options = $(this).metadata();
	options.noblock = true;
	options.target = $('<div><img src="images/ajax-loader.gif"/></div>').attr('title', title).dialog({
		modal: true, minWidth: width||650, minHeight: height||500,
		close: function(event, ui) { $(this).remove(); }
	});
	return true;
}
</script>
</head>
<body>
<fieldset>
<legend><h2>View Settings</h2></legend>
<div id="subpage_view">
<? output_subpage_view($_PAGE); ?>
</div>
<hr/>
<div id="subpage_cols">
<? output_subpage_cols($_PAGE); ?>
</div>
<br/>
<div id="subpage_charts">
<? output_subpage_charts($_PAGE); ?>
</div>
<br/>
</fieldset>
<div align="right"><a href="edit_tab.php" class="img_btn"><img src="images/back.png" width="24" border="0"/> Back</a></div>
</body>
</html>