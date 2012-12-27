<?
function output_subpage_view(&$_PAGE) {
	if ($_PAGE['view_id'])
		$view = db_fetch_assoc(db_query('SELECT * FROM virtual_view WHERE id=? AND user_id=?',array($_PAGE['view_id'],sess_get_uid())));
?>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="view"/>
<table cellpadding="3">
<tbody>
<tr><td>Name</td><td><input type="text" name="name" value="<?=q($view['name'])?>" class="required"/></td></tr>
<tr><td>Description</td><td><textarea name="description" cols="50"><?=q($view['description'])?></textarea></td></tr>
<tr><td>Default range</td><td><input type="text" name="day_range_default" value="<?=ors($view['day_range_default'],0)?>" size="3" class="number"/>days
<span class="footnote">(system default: 0)</span><span for="day_range_default"></span></td></tr>
<tr valign="top"><td></td>
<td align="right">

<button type="reset" class="img_btn" title="Reset"><img src="images/reload.png" height="24"/> Reset</button>
<button type="submit" class="img_btn {post:{action:'update'}}" title="Update"><img src="images/accept.png" width="24" class="hover"/> Update</button>

</td></tr>
</tbody>
</table>
</form>
<div class="page_msg"><?=get_ajax_msgs($_PAGE['page_msg'])?></div>
<?}?>
<?
function output_subpage_cols(&$_PAGE) {
	if ($_POST['view'])
		$view = db_fetch_assoc(db_query('SELECT * FROM view WHERE name=?',array($_POST['view'])));
	$v_cols = db_query('SELECT view_id,view.name,col_name,alias,virtual_view_item.id,seq FROM virtual_view_item, view WHERE view_id=view.id AND virtual_view_id=? ORDER BY seq',array($_PAGE['view_id']))->fetchAll(PDO::FETCH_ASSOC);
	if ($view) {
		$cols = $exclude = array();
		foreach ($v_cols as $row) {
			if ($view['id'] == $row['view_id'])
				$exclude[] = $row['col_name'];
		}
		$sth = db_query(str_replace('%where_and_more%', ' AND 0', $view['sql']));
		foreach(range(0, $sth->columnCount() - 1) as $i) {
			$meta = $sth->getColumnMeta($i);
			if ($i == 0 || !in_array($meta['name'], $exclude))
				$cols[] = $meta['name'];
		}
		array_shift($cols);
	}
?>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="cols"/>
<table cellpadding="3" border="1" style="min-width: 500px;">
<caption style="min-width: 500px"><h3>Columns</h3></caption>
<col/><col/><col/><col/><col width="32"/><col width="32"/>
<thead>
<?if (count($v_cols) > 0) {?>
<tr><th width="30">S/N</th><th>View Name</th><th>Column</th><th>Alias</th><th colspan="2" width="64"></th></tr>
<tr class="nodrag"><td align="right">1</td><td></td><td>[Primary Key]</td><td></td><td colspan="2"></td></tr>
<?}?>
</thead>
<tbody id="tbl-cols">
<?foreach ($v_cols as $i => $row) {?>
<tr seq="<?=$row['seq']?>"><td align="right"><?=$i+2?></td>
<td><?=q($row['name'])?></td>
<td><?=q($row['col_name'])?></td>
<td><?=q($row['alias'])?></td>
<td><button type="submit" class="img_btn {post:{action:'delete',col_id:<?=$row['id']?>}}" title="Delete"><img  src="images/delete.png" width="16"/></button></td>
<td class="dragHandle" align="center"><img src="images/updown2.gif" width="16" class="dragHandle" align="absmiddle"/></td>
</tr>
<?}?>
</tbody>
<tfoot>
<tr><td colspan="6">
<div><b>Add Column</b></div>
<div><table cellpadding="0"><tr>
<td><div>View: <input name="view" <?=$view['id']?'readonly="readonly"':''?> id="sel_view" size="45" value="<?=q($_POST['view'])?>" style="padding-right: 20px"/></div></td>
<td><?if ($view['id']) {?>
<img src="images/delete.png" width="16" onclick="reset_view(this)" title="Clear" class="btn" style="margin-left: -22px;"/>
<?}?></td>
</tr></table></div>
<?if ($view['id'] && count($cols)) {?>
<div>Columns: <select name="col">
<?foreach ($cols as $v) {?>
<option><?=q($v)?></option>
<?}?>
</select></div>
<div>
<span>Alias: <input type="text" name="alias" maxlength="128"/></span>
<span style="float: right">
<input type="hidden" name="view_id" value="<?=$view['id']?>"/>
<button type="button" class="img_btn" onclick="reset_view($('img.btn',this.form))" title="Cancel"><img src="images/cancel.png" width="24"/> Cancel</button>
<button type="submit" class="img_btn {post:{action:'add_col'}}" title="Add"><img src="images/add.png" width="24"/> Add</button>
</span>
</div>
<?} elseif ($view['id']) {?>
<div><label class="error">No more columns to add</span></div>
<?}?>
</td></tr>
</tfoot>
</table>
</form>
<div class="page_msg"><?=get_ajax_msgs($_PAGE['page_msg'])?></div>
<?}?>
<?
function output_subpage_charts(&$_PAGE) {
	$v_charts = db_query('SELECT id,name,options FROM virtual_view_chart WHERE view_id=?',array($_PAGE['view_id']))->fetchAll(PDO::FETCH_ASSOC);
?>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="charts"/>
<table cellpadding="3" border="1" style="min-width: 500px">
<caption style="min-width: 500px"><h3>Charts</h3></caption>
<thead>
<?if (count($v_charts) > 0) {?>
<tr><th width="30">S/N</th><th width="250">Name</th><th>Type</th><th width="75"></th></tr>
<?}?>
</thead>
<tbody>
<?foreach ($v_charts as $i => $chart) { $opt=json_decode($chart['options'],TRUE);?>
<tr><td align="right"><?=$i+1?></td>
<td><?=q($chart['name'])?></td>
<td><?=q($opt['Type'])?></td>
<td><button type="submit" class="img_btn {post:{action:'delete',chart_id:<?=$chart['id']?>}}" title="Delete"><img src="images/delete.png" width="16"/></button>
<button type="submit" class="img_btn {post:{page:'chart',chart_id:<?=$chart['id']?>},beforeSubmit:{open_modal:['Chart Settings']}}" title="Edit"><img src="images/setting.png" width="16"/></button></td>
</tr>
<?}?>
</tbody>
<tfoot><tr><td colspan="4" align="right"><button type="submit" class="img_btn {post:{page:'chart'},beforeSubmit:{open_modal:['Create Chart']}}"><img src="images/chart.png" width="24"/><img src="images/add.png" class="icon-overlay2"/> Create Chart</button></td></tr></tfoot>
</table>
</form>
<div class="page_msg"><?=get_ajax_msgs($_PAGE['page_msg'])?></div>
<?}?>
<?
function output_error_no_cols() {
?>	
<div><label class="error">Please add coumns into this view before creating chart.</label></div>
<hr/>
<div align="right"><button type="button" class="img_btn" onclick="close_dialog(this)"><img src="images/cancel.png"/> Close</button></div>
<?}?>
<?
function output_subpage_chart(&$_PAGE) {
	if ($_PAGE['chart_id'] = $_POST['chart_id']) {
		$chart = db_fetch_assoc(db_query('SELECT * FROM virtual_view_chart WHERE id=?', array($_PAGE['chart_id'])));
		$chart_opt = json_decode($chart['options'],TRUE);
		$_PAGE['view_id'] = $chart['view_id'];
		$selected1[$chart_opt['Type']] = ' selected="selected"';
		foreach (explode(',',$chart['columns']) as $c)
			$selected2[$c] = ' selected="selected"';
		$checked1[ors($chart_opt['Aggregation'],'')] = ' checked="checked"';
		$checked2[ors($chart_opt['Template']['Options']['scaleStart0'],0)] = ' checked="checked"';
		$size = explode('x',$chart_opt['Template']['Size']);
	} else {
		$selected1['1d1'] = ' selected="selected"';
		$checked1[''] = $checked2[1] = ' checked="checked"';
		$_PAGE['view_id'] = $_GET['id'];
	}
	if (!db_query_for_one('SELECT COUNT(*) FROM virtual_view WHERE id=? AND user_id=?',array($_PAGE['view_id'],sess_get_uid())))
		stop('Access denied!');
	$v_cols = db_query('SELECT view_id,view.name,col_name,alias,virtual_view_item.id,seq FROM virtual_view_item, view WHERE view_id=view.id AND virtual_view_id=? ORDER BY seq',array($_PAGE['view_id']))->fetchAll(PDO::FETCH_ASSOC);
	if (count($v_cols) == 0) {
		output_error_no_cols();
		return;
	}
?>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="charts"/>
<table>
<tr><td>Title:</td><td><input name="name" type="text" size="30" class="required" maxlength="128" value="<?=q($chart['name'])?>"/></td></tr>
<tr><td>Type:</td><td><select name="type" class="imageselect required" id="ct_<?=time()?>" style="width: 200px;">
<?foreach (explode(',','1d1,1d2,1d3,2d1,2d2,2d3') as $t) {?>
  <option value="<?=$t?>" title="<?=pl('refs/chart_'.$t.'.png')?>"<?=$selected1[$t]?>><?=$t?></option>
<?}?>
</select></td></tr>
<tr><td>Columns:</td><td><select name="columns[]" class="multiselect {required:true, messages:{required:'At least 1 column required'}}" multiple="multiple" style="width:500px; height: 100px;">
<?foreach ($v_cols as $i => $row) {?>
<option<?=$selected2[ors($row['alias'],$row['col_name'])]?>><?=q(ors($row['alias'],$row['col_name']))?></option>
<?}?>
</select></td></tr>
<tr><td></td><td><span for="columns[]"></span></td></tr>
<tr><td>Aggregation:</td><td><input type="radio" name="aggregation" value=""<?=$checked1['']?>/><label>None</label>
<input type="radio" name="aggregation" value="sum"<?=$checked1['sum']?>/><label>Sum</label></td></tr>
<tr><td>Row limit:</td><td><input name="row_limit" type="text" size="3" class="number" value="<?=ors($chart_opt['RowLimit'],0)?>"/></td></tr>
<tr><td>Starts from 0:</td><td><input type="radio" name="start_zero" value="1"<?=$checked2[1]?>/><label>Yes</label>
<input type="radio" name="start_zero" value=" value="0"<?=$checked2[0]?>/><label>No</label></td></tr>
<tr><td>Size:</td><td><input name="width" type="text" size="3" maxlength="4" class="number" value="<?=ors($size[0],1000)?>" /> X <input name="height" type="text" size="3" maxlength="4" class="number" value="<?=ors($size[1],400)?>" />
<span for="width"></span></td></tr>
<tr><td>Description:</td><td><textarea name="description" cols="50"><?=q($chart['description'])?></textarea></td></tr>
</table>
<hr/>
<div align="right">
<button type="button" class="img_btn" onclick="close_dialog(this)"><img src="images/cancel.png"/> Cancel</button>
<?if ($_PAGE['chart_id']) {?>
<input type="hidden" name="chart_id" value="<?=$_PAGE['chart_id']?>"/>
<button type="submit" class="img_btn {post:{action:'update'}}"><img src="images/accept.png"/> Update</button>
<?}else{?>
<button type="submit" class="img_btn {post:{action:'add'}}"><img src="images/accept.png"/> Create</button>
<?}?>
</div>
</form>
<div class="hidden">
<?if ($_PAGE['PRELOAD']) foreach ($_PAGE['PRELOAD'] as $i) {?>
<img src="<?=$i?>"/>
<?}?>
</div>
<?}?>
