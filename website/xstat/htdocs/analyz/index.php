<?php

require('../common.inc.php');
page_start();
$user_id=sess_get_uid();

if(empty($user_id)){
	redirect('..');
	exit();
}




?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>MoAnalatics</title>
        <link rel="stylesheet" href="../css/jquery-ui.css" type="text/css" media="all" />
        <link rel="stylesheet" href="./css/jquery.treeview.css" />
        <link rel="stylesheet" href="../common.css" />
        <script src="./js/jquery.min.js" type="text/javascript">
        </script>
        <script src="./js/jquery-ui.min.js" type="text/javascript">
        </script>
        <script src="./js/jquery.cookie.js" type="text/javascript">
        </script>
        <script src="./js/jquery.treeview.js" type="text/javascript">
        </script>
        <script src="./js/jquery.blockui.js" type="text/javascript">
        </script>
        <style type="text/css" title="">        
                    
            body {
                font-size: 12px;
            }
            
            #tabs {
                font-size: 100%;
            }
            
            th, td {
                font-size: 8pt;
            }
            
            #collection_stat_table {
                border-collapse: collapse;
                background-color: white;
            }
            
            #collection_stat_table td {
                white-space: nowrap;
                text-align: right;
                padding: 3px;
            }
            
            #collection_stat_table tbody tr:nth-child(odd) { 
                                                                                    background-color: #EEE; 
                                                                                    } /* IE not support */
                                                                                    
                                                                                    .chart_title {
                                                                                    text-align: right;
                                                                                    padding-right: 5px;
                                                                                    }
                                                                                    
                                                                                    .chart_block {
                                                                                    margin-bottom: 10px;
                                                                                    }
                                                                                    
                                                                                    .chart_descr {
                                                                                    padding: 15px 10px;
                                                                                    }
                                                                                    
                                                                                    .view_descr {
                                                                                    padding: 15px 0px;
                                                                                    }
                                                                                    
                                                                                    .right_link {
                                                                                    margin-top: 5px;
                                                                                    float: right;
                                                                                    }
                                                                                    
                                                                                    .highlight {
                                                                                    background-color: #FFFF96 !important;
                                                                                    }
                                                                                    
                                                                                    .highlight2 {
                                                                                    background-color: #FFFF96 !important;
                                                                                    }
                                                                                    
                                                                                    input.groovybutton {
                                                                                    font-size: 12px;
                                                                                    font-family: Arial, sans-serif;
                                                                                    font-weight: bold;
                                                                                    color: #444444;
                                                                                    background-color: #EEEEEE;
                                                                                    border-style: double;
                                                                                    border-color: #999999;
                                                                                    border-width: 3px;
                                                                                    }
                                                                                        
                                                                                    
                                                                                    table { border-collapse: collapse; }
                                                                                    td, th { border: 1px solid #000000; vertical-align: baseline; }
                                                                                      td:hover {cursor:pointer;}   
                                                                                    input.groovybutton
                                                                                     {
                                                                                     font-size:12px;
                                                                                     font-family:Arial,sans-serif;
                                                                                     font-weight:bold;
                                                                                     color:#444444;
                                                                                     background-color:#EEEEEE;
                                                                                     border-style:double;
                                                                                     border-color:#999999;
                                                                                     border-width:3px;
                                                                                     }                
                                                                                                                                            
                                                                                           
                                                                                                                                
                                                                                                                    
                                                                                                        
                                                                                            
                                                                                
                                                                    
                                                        
                                            
                                
                    
        </style>
    </head>
    <body>
        <script type="text/javascript">
            
            function stopBubble(e){
                e = e || window.event;
                if (e.stopPropagation) {
                    e.stopPropagation();
                }
                else {
                    e.cancelBubble = true;
                }
                if (e.preventDefault) {
                    //e.preventDefault();
                }
                else {
                    //e.returnValue = false;
                    e.returnValue = true;
                }
            }
            
            function refreshCollectionList(){
                var data = $.getJSON('data.php?action=collections_my', function(data){
                    var tree_html = '<div style="float:right;">' +
					'<button type="button" class="img_btn" id="btn_compare"><img src="../images/action.png" height="24"/> Compare</button>&nbsp;'+
                    '</div>' +
                    '<h3>My User Groups</h3><ul id="tree"><li>';
                    var last_serial_number = '0000000000';
                    var last_group_number = data[0] && data[0]['group_number'] || '';
                    var collection = {};
                    
                    for (k in data) {
                    
                    
                        var row = data[k];
                        collection[row['id']] = row;
                        
                        var current_serial_number = row['serial_number'];
                        
                        
                        if (current_serial_number.length == last_serial_number.length &&
                        current_serial_number.slice(0, current_serial_number.length - 10) != last_serial_number.slice(0, last_serial_number.length - 10)) {
                            tree_html += '</li></ul></li>';
                            last_serial_number = last_serial_number.slice(0, last_serial_number.length - 20);
                        }
                        
                        
                        
                        if (current_serial_number.length == last_serial_number.length) {
                            tree_html += '</li><li>';
                        }
                        
                        if (current_serial_number.length > last_serial_number.length) {
                            tree_html += '<ul><li>';
                            if (current_serial_number.length - last_serial_number.length == 20) {
                                tree_html += '<span style="color:grey;">&nbsp;grouped by "' + row['clustered_columns'] + '"</span><ul><li>';
                            }
                        }
                        
                        
                        if (current_serial_number.length < last_serial_number.length) {
                            var len = (last_serial_number.length - current_serial_number.length) / 20;
                            for (var i = 0; i < len; i++) {
                                tree_html += '</li></ul></li></li></ul></li>';
                            }
                            tree_html += '<li>';
                        }
                        
                        var img = '<img src="images/user_sub_group.png" style="margin-bottom:-3px; margin-left:3px; margin-right:3px;" />';
                        if (row['serial_number'].length == 10) {
                            img = '<img src="images/user_group.png" style="margin-bottom:-3px; margin-left:3px; margin-right:3px;" />';
                        }
                        var collection_name = row['collection_name'] || row['collection_predefined_name'];
                        
                        if (row['serial_number'].length == 10) {
                            collection_name = '<b>' + row['collection_predefined_name'] + '</b>&nbsp;&nbsp; <font color="grey">total=' +
                            row['element_count'] +
                            ', ' +
                            row['created_on'].slice(0, 10) +
                            '</font>';
                        }
                        else {
                            collection_name = '<b>avg=' +
                            row[row['clustered_columns']].replace(/(\.\d{2})\d+/ig, '$1') +
                            '</b>, <a title="' +
                            row['element_count'] +
                            ' user(s)" style="color:grey" onclick="return false;">' +
                            (row['element_count'] * 100 / collection[row['parent_collection_id']]['element_count']).toString().replace(/(\.\d{2})\d+/ig, '$1') +
                            '%' +
                            '</a>';
                        }
                        if (row['name']) {
                            collection_name = '<b>' + row['name'] + '</b>&nbsp;&nbsp;' + collection_name.replace('<b>', '<font color="grey">').replace('</b>', '</font>');
                        }
                        var col_name_plain = collection_name.replace(/<.*?>/ig, '').replace(/&nbsp;/ig, '').replace('"', '').replace("'", '');
                        
                        var checkbox = '&nbsp;<input class="col_select" title="select to compare (' + row['id'] + ')" type="checkbox" name="col_id" value="' + row['id'] + '" col_name="' + col_name_plain + '"/>';
                        //alert(checkbox);
                        var button = '<div class="div_btn">'
                        
                        tree_html += '<span>' + img + collection_name + checkbox + '</span>';
                        last_serial_number = current_serial_number;
                    }
                    tree_html += '</li></ul>';
                    //alert(tree_html);
                    
                    $('#collection_list_tree').html(tree_html.replace(/<li><\/li>/ig, ''));
                    
                    $("#tree").treeview({
                        animated: "fast",
                        collapsed: true,
                        unique: true,
                        persist: "cookie",
                        toggle: function(){
                            window.console && console.log("%o was toggled", this);
                        }
                    });
                    
                    
                    $('.col_select').click(function(e){
                        stopBubble(e);
                        return true;
                    })
                    
                    $('#btn_compare').click(function(e){
                        stopBubble(e);
                        refreshCollectionStat();
                        
                        return true;
                    })
                    
                    $('#btn_create_collection').click(function(e){
                        stopBubble(e);
                        newCollection();
                        
                        return false;
                    })
                });
                
            }
            

            function do_create_collection(){
                var url = 'data.php?action=create_init_collection&predefined_collection_name=' +
                $('#init_col_select').val();
                $.unblockUI();
                $.blockUI({
                    message: '<h4>Creating the initial user group...</h4>'
                });
                $.ajax({
                    url: url,
                    success: function(data){
                        $.unblockUI();
                        refreshCollectionList();
                        //$('#collection_view').trigger('click');
                    },
                    error: function(){
                        $.unblockUI();
                    },
                    timeout: 600000
                });
            }
            
            function refreshCollectionStat(){
            
                var url = 'data.php?action=collections_stat';
                var checked = 0;
                $('.col_select').each(function(i, e){
                    if (e.checked) {
                        url += '&ids[]=' + e.value.toString() + "&col_names[]=" + e.getAttribute('col_name');
                        
                        checked++;
                    }
                });
                
                //alert(url);
                if (!checked) {
                    alert("No collection selected !");
                    return false;
                }
                
                //$.blockUI({ message: '<h3>Loading statistics...</h3>' });
                var table_html = $.get(url, function(data){
                    $('#collection_stat_table').html(data + '<br/><br/><font color="grey">Double click on the cell to start auto-grouping.</font><br/>');
                    
                    if ($.browser.msie) {
                        $('#stat_table tbody tr:nth-child(odd)').css('backgroundColor', '#EEE');
                    }
                    
                    $("#stat_table tbody tr").click(function(){
                        $(this).toggleClass('highlight');
                    });
                    $("#stat_table th").click(function(){
                        $("#stat_table tr :nth-child(" + ($(this).index() + 1) + ")").toggleClass('highlight2');
                    });
                    
                    $('#comparison_view').trigger('click');
                    //$.unblockUI();
                    
                    $('.data_cel').dblclick(function(e){
                        var span = this;
                        while (span.tagName.toLowerCase() != 'span') {
                            span = span.parentNode;
                        }
                        var init_id = span.getAttribute('init_col_id');
                        var parent_id = span.getAttribute('col_id');
                        var column = span.getAttribute('column_name');
                        
                        if (confirm('System will do auto-grouping on [' + span.getAttribute('column_name') + '] and generate new collections. It may take 1 min if the parent collection is big. Continue processing?')) {
                            var targetCol = 0;
                            while (!(targetCol.toString().search(/^\d+$/ig) > -1 && targetCol > 1 && targetCol <= 10)) {
                                targetCol = prompt('Input the number of groups to cluster [2-10]:');
                            }
                            
                            $.blockUI({
                                message: '<h3>Clustering in progress...</h3>'
                            });
                            
                            var url = 'data.php?action=cluster&init_collection_id=' + init_id + '&parent_collection_id=' + parent_id + '&collection_count=' + targetCol + '&column=' + column;
                            
                            $.ajax({
                                url: url,
                                success: function(data){
                                    refreshCollectionList();
                                    $('#collection_view').trigger('click');
                                    $.unblockUI();
                                },
                                error: function(){
                                    $.unblockUI();
                                },
                                timeout: 600000
                            });
                            
                            
                        }
                        
                    });
                });
            }
            
            function newCollection(){
                var url = 'data.php?action=existing_init_collection';
                
                $.ajax({
                    url: url,
                    success: function(data){
                        $.blockUI({
                            message: data
                        });
                    }
                });
                
            }
            
            $(function(){
                $("#tabs").tabs({
                    collapsible: true,
                    ajaxOptions: {
                        error: function(xhr, status, index, anchor){
                            $(anchor.hash).html("Couldn't load this tab. We'll try to fix this as soon as possible. If this wouldn't be a demo.");
                        }
                    }
                });
                refreshCollectionList();
                //refreshCollectionStat();
            
            });
        </script>

		<div style="float:right;">
			<button type="button" class="img_btn" onclick="window.top.location='../home.php';"><img src="../images/home.png" height="24"/> Home</button>
		</div>

        <h2>My Analatics</h2>
        <div id="tabs">
            <ul>
                <li>
                    <a id="collection_view" href="#collection_list">Collection List</a>
                </li>
                <li>
                    <a id="comparison_view" href="#comparison">Comparison</a>
                </li>
            </ul>
            <div id="collection_list">
                <p id="collection_list_tree">
                </p>
				<div style="float:right;"><button id="btn_create_collection" type="button" class="img_btn"><img src="../images/plus.png" height="24"/> New Initial Collection</button></div>
				<div style="clear:both;"></div>
            </div>
            <div id="comparison">
                <p id="collection_stat_table">
                    No collection selected. You may select some from the Collection List tab and click Compare button. 
                </p>
            </div>
        </div>
    </body>
