<?php
function get_comment_attr($key, $notes) {
	if ($notes)
		return sprintf(' class="commented {mkey:\'%s\'}" title="%s"', q($key), q(trim(strip_tags(preg_replace('# class="user_name">[^<]*|>:#','>',$notes)))));
	if (preg_match('/^\d{4}-\d{2}-\d{2}$/',$key))
		return sprintf(' class="commentable {mkey:\'%s\'}"', q($key));
	return '';
}
function output_subpage_table(&$_PAGE) {
	global $view, $values, $params, $user_name;
	if (ends_with($user_name,'@mozat.com'))
		$notes = db_query_for_assoc('SELECT mkey,notes FROM notes WHERE mkey BETWEEN :date1 AND :date2', $params);
?>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="#tabs-1"/>
<table border="1" class="main_table">
<thead>
<tr>
<th><?=q($view['key'])?></th>
<?foreach ($view['cols'] as $col) {?>
<th><?=q($col['name'])?></th>
<?}?>
</tr>
</thead>
<tbody>
<?foreach ($values as $i => $row) {?>
<tr>
<?foreach (range(0, count($view['cols'])) as $j) {?>
<td<?=$j==0&&ends_with($user_name,'@mozat.com')?get_comment_attr($row[$j], $notes[$row[$j]]):''?>><span class="hidden"><?=q($notes[$row[$j]])?></span><?=n($row[$j])?></td>
<?}?>
</tr>
<?}?>
</tbody>
</table>
<?if ($view['description']){?>
<div class="view_descr"><?=nl2br($view['description'])?></div>
<?}?>
<?if(is_admin()){?>
<div class="right_link"><a href="#" onclick="window.open('chart_create.php?&id=<?=$_PAGE['view_id']?>');return false;" target="_blank" style="color:gray;text-decoration:none;">create chart</a>
</div>
<div style="clear:both;"></div>
<?}?>
<span id="add_comment"><button type="button" class="img_btn" title="Add Notes"><img src="images/comment_add.png" width="14"/></button></span>
<span id="edit_comment"><button type="button" class="img_btn" title="Append Notes"><img src="images/comment_edit.png" width="14"/></button></span>
</form>

<div title="Add Notes" id="comment_add">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="#tabs-1"/>
<div>Date: <span id="comment_date"></span></div>
<div style="height: 90px">
<textarea rows="5" cols="50" name="notes" class="required"></textarea>
</div>
<hr/>
<div align="right"><button type="button" class="img_btn" onclick="close_dialog(this)"><img src="images/cancel.png" width="24"/> Cancel</button>
<button type="submit" class="img_btn {post:{action:'add'}}"><img src="images/add.png" width="24"/> Add</button></div>
</form>
</div>

<div title="Append Notes" id="comment_append">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="#tabs-1"/>
<div>Date: <span id="comment_date"></span></div>
<div>Notes:  <br/><span id="comment_content"></span></div>
<div style="height: 90px">
<textarea rows="5" cols="50" name="notes" class="required"></textarea>
<div style="color:gray; float:right; font-size:10px;">* operator name is required&nbsp;&nbsp;&nbsp;&nbsp;</div>
</div>
<hr/>
<div align="right"><button type="button" class="img_btn" onclick="close_dialog(this)"><img src="images/cancel.png" width="24"/> Cancel</button>
<button type="submit" class="img_btn {post:{action:'append'}}"><img src="images/add.png" width="24"/> Append</button></div>
</form>
</div>
<?	
}
?>