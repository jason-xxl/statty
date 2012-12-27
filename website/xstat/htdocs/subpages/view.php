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
<?foreach ($view['cols'] as $i => $k) {?>
<th><?
#$col_title_str=q($k);
$col_title_str=$k;
if(preg_match('/\\b(UV|Unique)\\b/i', $col_title_str)>0){
	if($_GET['group_by']=='weekly'){
		$col_title_str=	preg_replace('/\\b1d\\b/','7d',$col_title_str,1);
	}else if($_GET['group_by']=='monthly'){
		$col_title_str=	preg_replace('/\\b1d\\b/','30d',$col_title_str,1);
	}
}
?><?=$col_title_str?></th>
<?}?>
</tr>
</thead>
<tbody>
<?foreach ($values as $i => $row) {?>
<tr>
<?foreach ($row as $j => $v) {?>
<td<?=$j==0&&ends_with($user_name,'@mozat.com')?get_comment_attr($v, $notes[$v]):''?>><span class="hidden"><?=q($notes[$v])?></span><?=$j==0?n($v):n($v)?></td>
<?}?>
</tr>
<?}?>
</tbody>
</table>
</form>
<?if ($view['description']){?>
<div class="view_descr"><?=nl2br($view['description'])?></div>
<?}?>
<!--
<?if(is_admin()){?>
<div class="right_link"><a href="#" onclick="window.open('chart_create.php?&id=<?=$_PAGE['view_id']?>');return false;" target="_blank" style="color:gray;text-decoration:none;">create chart</a>
</div>
<div style="clear:both;"></div>
<?}?>
-->
<?if(is_admin() && ($user_name=='xianli@mozat.com' || $user_name=='yonghao@mozat.com')){?>

<div class="right_link"><a href="http://127.0.0.1:15881/phpmyadmin-2/tbl_change.php?db=mozat_stat&table=view&where_clause=%60view%60.%60id%60+%3D+<?=$_PAGE['view_id']?>&clause_is_unique=1&sql_query=SELECT+*+FROM+%60view%60&goto=sql.php&default_action=update&server=3&token=24f1b6dd7e86666b8ca76620c6318233" target="_blank" style="color:gray;text-decoration:none;">edit definition in PMA-2</a>
</div>
<div style="clear:both;"></div>
<?}?>
<span id="add_comment"><button type="button" class="img_btn" title="Add Notes"><img src="images/comment_add.png" width="14"/></button></span>
<span id="edit_comment"><button type="button" class="img_btn" title="Append Notes"><img src="images/comment_edit.png" width="14"/></button></span>

<div title="Add Notes" id="comment_add">
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="#tabs-1"/>
<div>Date: <span id="comment_date"></span></div>
<div style="height: 90px">
<textarea rows="5" cols="80" name="notes" class="required"></textarea>
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
<div style="width:97%; word-wrap: break-word;">Notes: <br/><span id="comment_content"></span></div>
<div style="height: 90px">
<textarea rows="5" cols="80" name="notes" class="required"></textarea>
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