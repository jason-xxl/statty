<?php
set_time_limit(180);
require ('../common.inc.php');
require ('./translate_data.php');

page_start();

$user_id = sess_get_uid();

//$_GET['action'] = 'collections_stat';
//$_GET['ids'] = array (
//	1,
//	2,
//	3,
//	4,
//	5,
//	6
//);

//$_GET['action'] = 'collections_my';
//var_dump($_GET);

switch ($_GET['action']) {
	case "collections_stat" :
		$avg = array ();
		if (is_array($_GET['ids'])) {
			foreach ($_GET['ids'] as $i) {
				$collection_stat = db_fetch_assoc(db_query('SELECT * FROM `mozat_clustering`.`collection_avg_general` where `collection_id`=? order by `collection_id` desc', array (
					$i
				)));
				foreach ($collection_stat as $k => $v) {
					if (strpos($k, 'fig') === 0 || $k == 'collection_id') {
						unset ($collection_stat[$k]);
					}
				}
				unset ($collection_stat['collection_id']);
				ksort($collection_stat);
				$avg[$i] = $collection_stat;
			}
		}
		
		$collection=array();
		
		$return = array ();
		$html = '<table id="stat_table">';

		if (!empty ($avg)) {

			//title
			$html.='<thead><tr>';
			
			$return[0] = array ();
			$return[0][] = '';
			$html.='<th>&nbsp;</th>';
			
			foreach ($_GET['ids'] as $k => $v) {
				$return[0][] = $v;
				$html.='<th>'.htmlentities($_GET['col_names'][$k]).'</th>';
			}
			$html.='</tr></thead>';

			//data
			$html.='<tbody>';
			
			foreach ($avg[$_GET['ids'][0]] as $k => $v) {
				$html.='<tr>';
				$tmp = array (
					$k
				);
				$colname=$k;
				$html.='<td style="text-align:left;">'.htmlentities($k).'</td>';
				foreach ($_GET['ids'] as $id) {
					$tmp[] = $avg[$id][$k];
					$val=data_to_text($avg[$id][$k],$k);
					
					if(empty($collection[$id])){
						$collection[$id]=db_fetch_assoc(db_query('SELECT * FROM `mozat_clustering`.`collection` where `id`=?', array (
							$id
						)));
					}
					
					
					$html.="<td><span class='data_cel' parent_col_id='".$collection[$id]["parent_collection_id"]
					."' init_col_id='".$collection[$id]["init_collection_id"]."' col_id='$id' column_name='$colname'>".htmlentities($val).'</span></td>';
				}
				$html.='</tr>';
				$return[] = $tmp;
			}
			
			$html.='</tbody>';
		}

		$html .= '</table>';
		echo $html;
		//echo json_encode($return);
		break;

	case "collections_my" :
		$collection_my = array ();
		$query = db_query('SELECT * FROM `mozat_clustering`.`collection` a join `mozat_clustering`.`collection_avg` b on a.id=b.collection_id where `owner_id`=? order by `init_collection_id` desc,`serial_number` asc', array (
			$user_id
		));
		while ($row = db_fetch_assoc($query)) {
			$row[$row['clustered_columns']]=data_to_text($row[$row['clustered_columns']],$row['clustered_columns']);
			$collection_my[] = $row;
		};

		echo json_encode($collection_my);
		break;

	case "collection_delete" :
		$query = db_query('delete FROM `mozat_clustering`.`collection` where `owner_id`=? and `serial_number` like "'.preg_replace("/[^0-9]/g","",$_GET['serial_number']).'%" and owner_id = ? ', array (
			$user_id
		));

		echo '//ok';
		break;
		
	case "cluster":
		set_time_limit(3600);
		
		$owner_id=$user_id;
		$act="create_cluster_stat_derived_collection";
		$parent_collection_id=$_GET['parent_collection_id'];
		$init_collection_id=$_GET['init_collection_id'];
		$col_count=$_GET['collection_count'];
		$column=$_GET['column'];
		
		// args=['','1','create_cluster_stat_derived_collection','1','2','3','sns-add_other_as_friend']
		
		
		$command="E:\\RoutineScripts\\user_figure_crawler\\src\\collection_process.py $owner_id create_cluster_stat_derived_collection $init_collection_id $parent_collection_id $col_count $column";
		$command.=" > E:\\RoutineScripts\\user_figure_crawler\\src\\log\\collection_process-$owner_id-create_cluster_stat_derived_collection-$init_collection_id-$parent_collection_id-$col_count-$column.log";
		
		exec($command);
		
		echo "//done $command";
		break;
		
	case "create_init_collection":
		set_time_limit(3600);
		
		$owner_id=$user_id;
		$act="create_stat_init_collection";
		$name=$_GET['predefined_collection_name'];
		
		// args=['','1','create_stat_init_collection','STC_User_Unsub']
		
		
		$command="E:\\RoutineScripts\\user_figure_crawler\\src\\collection_process.py $owner_id create_stat_init_collection $name";
		$command.=" > E:\\RoutineScripts\\user_figure_crawler\\src\\log\\collection_process-$owner_id-create_stat_init_collection-$name.log";
		
		exec($command);
		
		echo "//done $command";
		break;
		
	case "existing_init_collection":
	
		$collection_init = array ();
		$query = db_query('SELECT user_group_name FROM `mozat_clustering`.`collection_predefined` where user_group_name like "%vodafone%" order by `user_group_name` asc');
		
		$html='<div><h4>Select the initial user group from the below list.</h4><select id="init_col_select">';
		
		
		while ($row = db_fetch_assoc($query)) {
			$collection_init[] = $row;
			$html.='<option value="'.$row['user_group_name'].'">'.$row['user_group_name'].'</option>';
		};

		$html.='</select>&nbsp;&nbsp;<input class="groovybutton" type="button" value="Create" onclick="do_create_collection()">&nbsp;&nbsp;<a onclick="$.unblockUI();return false;">cancel</a><br/><br/></table>';
		//echo json_encode($collection_init);
		
		echo $html;
		break;
}
?>

