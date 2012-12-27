<?
function output_subpage_body(&$_PAGE) {
	$views2 = db_query('SELECT *,(SELECT COUNT(*) FROM virtual_view_item WHERE virtual_view_id=virtual_view.id) AS count FROM virtual_view WHERE user_id=?', array(sess_get_uid()))->fetchAll(PDO::FETCH_ASSOC);
?>
<form action="<?=$_PAGE['.']?>" method="post">
<input type="hidden" name="page" value="views"/>
<table cellpadding="3" border="1" style="min-width: 500px;">
<caption style="min-width: 500px;"><h3>My Views</h3></caption>
<thead><tr><th width="30">S/N</th><th>View Name</th><th width="50">Columns</th><th width="110"></th></tr></thead>
<tbody>
<?foreach ($views2 as $i => $row) {?>
<tr>
<td align="right"><?=$i+1?></td>
<td><a href="view_v.php?id=<?=$row['id']?>" target="_blank" title="<?=q($row['description'])?>"><?=q($row['name'])?></a></td>
<td align="right"><?=$row['count']?$row['count']+1:0?></td>
<td><button type="submit" class="img_btn {post:{action:'delete',view_id:<?=$row['id']?>},confirm:'Are you sure to delete?'}" title="Delete"><img src="images/delete.png" width="16" border="0"></button>
<a href="edit_view_v.php?id=<?=$row['id']?>" title="Edit" class="img_btn"><img src="images/setting.png" width="16" border="0"/></a>
<?if (!$row['enabled'] || !$_PAGE['email']) {?>
<?}elseif ($row['subscribed']) {?>
<button type="submit" class="img_btn {post:{action:'unsubscribe',view_id:<?=$row['id']?>}}" title="Unsubscribe"><img src="images/mail.png" width="16" border="0"/><img src="images/accept.png" class="icon-overlay"/></button>
<?}else{?>
<button type="submit" class="img_btn {post:{action:'subscribe',view_id:<?=$row['id']?>}}" title="Subscribe"><img src="images/mail.png" width="16" border="0"/></button>
<?}?></td>
</tr>
<?}?>
</tbody>
</table>
</form>
<?}?>
