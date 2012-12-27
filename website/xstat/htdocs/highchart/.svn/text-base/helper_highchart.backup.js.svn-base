
function _get_key_column_values(table_id){
	table_id=table_id||'table.main_table';
	result=[];
	rows=$(table_id+' tbody tr');
	for (var i=0;i<rows.length;i++){
		result[result.length]=$('td',rows[i]).first().html().replace(/^.*?(?:<\/span>)/ig,'');
	}
	return result;
}

function _generate_transposed_table(){
	var html=$('table.main_table').html();
	html=html.replace('<thead>','<tbody>').replace('</thead>\n<tbody>\n','').replace(/<span class="hidden"><\/span>/ig,'');
	html=html.replace(/<th>/ig,'<td>').replace(/<\/th>/ig,'</td>');
	html='<table id="main_table_temp" style="display:;">'+html+'</table>';
	//console.log(html);
	
	$('table.main_table').parent().append(html);
	
    var t = $('table#main_table_temp').eq(0);
    var r = t.find('tr');
    var cols= r.length;
    var rows= r.eq(0).find('td').length;
    var cell, next, tem, i = 0;
    var tb= $('<tbody></tbody>');

	//console.log(r,cols,rows,tb);
    while(i<rows){
        var cell= 0;
        var tem = $('<tr></tr>');
        while(cell<cols){
            var next=r.eq(cell++).find('td').eq(0);
            tem.append(next);
        }
        tb.append(tem);
        ++i;
    }
	//console.log(tb);

	var html_new='<tbody>'+tb.html()+'</tbody>';
	html_new=html_new.replace('<tbody>','<thead>').replace('</tr><tr>','</tr></thead><tbody><tr>');
	//console.log(rows);

	for(var j=0;j<cols;j++){
		html_new=html_new.replace('<td','<th').replace('</td>','</th>');
	}

    $('table#main_table_temp').html(html_new);
	console.log(html_new);
	//console.log($('table#main_table_temp').html());
}



function _get_cell_content(column_name,value_of_key_row,table_id){
	table_id=table_id||'table.main_table';
	column_names=$(table_id+' thead tr th');
	column_index=-1;
	for (var i=0;i<column_names.length;i++){
		if (column_names[i].innerHTML==column_name){
			column_index=i;
			break;
		}
	}
	if (column_index==-1) {
		return '';
	}
	rows=$(table_id+' tbody tr');
	var html='';
	for (var i=0;i<rows.length;i++){
		//console.log(rows[i].innerHTML);
		if (rows[i].innerHTML.indexOf(value_of_key_row+'</td>')==-1){
			continue;
		}
		html=rows[i].innerHTML;
		break;
	}

	
	//console.log(column_index);
	//console.log(value_of_key_row);
	//console.log(html);
	//console.log(html,column_index,value_of_key_row);

	target_cell=html.split('</td>')[column_index].replace('<td><span class="hidden"></span>','');
	//console.log(target_cell);

	return target_cell;
}


function _get_column_names(filter_pattern,table_id){
	table_id=table_id||'table.main_table';

	filter_pattern=filter_pattern||/.*/ig;
	result=[];

	column_names=$(table_id+' thead tr th');

	for (var i=0;i<column_names.length;i++){
		var name=column_names[i].innerHTML;
		var re=new RegExp(filter_pattern);
		if (re.exec(name)){
			result[result.length]=name;
		}
	}

	return result;
}



//console.log(_get_cell_content('Avg Chatroom Message Created in [13d,15d]','Online for 16d-18d'));
//console.log(_get_key_column_values());

function _add_tab(tab_name){
	if (!$('#tabs').attr('max_tab')) {
		$('#tabs').attr('max_tab',100);
	}
	var max_tab=$('#tabs').attr('max_tab');
	//console.log($('#tabs').attr('max_tab'));
	//$('#tabs ul').append('');
	max_tab+=1;
	$('#tabs ul').append('<li><a href="#tabs-'+max_tab.toString()+'"><span>'+tab_name.replace('_',' ')+'</span></a></li>');
	$('#tabs').append('<div id="tabs-'+max_tab+'" style="width:95%;">');
	$('#tabs').tabs({cookie: { expires: 30, name: "ui-tabs-v" }});
	
	//$('#tabs-'+max_tab).append('fhdjkaslfhjdsk');

	return 'tabs-'+max_tab;
}

//console.log(_add_tab('Test'));

function _get_number(str){
	//console.log(str);
	var p=/(\d+(\s*[,\.]\s*\d+)*)/ig;
	if (p.test(str)){
		//console.log(RegExp.$1);
		var temp=RegExp.$1.replace(/,/ig,'');
		if (temp.indexOf('.')>=-1){
			return parseFloat(temp);
		}else{
			return parseInt(temp);
		}
	}else{
		return 0;
	}
}

function _extract_cell_content(cell_content){
	var i=cell_content.lastIndexOf('>');
	if (i>=-1){
		return cell_content.substring(i+1);
	}
	return cell_content
}

//console.log(_extract_cell_content('<span class="hidden">&lt;span class="user_name"&gt;galal@mozat.com&lt;/span&gt;:&lt;span class="content"&gt;STC &amp; Buzz: Change Friend Invitation SMS content&lt;/span&gt;</span>2012-06-05'));
//console.log(_get_number('ff777,777,444.4fd'));

function add_highcharts_basic_line_chart_gray(tab_name,column_names,marginRight,reverse_key_column,reverse_table){
	marginRight=marginRight||200;
	reverse_key_column=reverse_key_column===null?false:reverse_key_column;
	reverse_table=reverse_table===null?false:reverse_table;
	var table_id;

	if (reverse_table){
		_generate_transposed_table();
		table_id='table#main_table_temp';
	}else{
		table_id='table.main_table';
	}

	var container_id=_add_tab(tab_name);
	var yAxis={
		title: {
			text: ''
		},
		plotLines: [{
			value: 0,
			width: 1,
			color: '#808080'
		}]
	};
	
	var key_column_values=_get_key_column_values(table_id);
	
	for (var i=0;i<key_column_values.length;i++ ){
		key_column_values[i]=_extract_cell_content(key_column_values[i]);
		
		//process common pattern

		var p=/^(\d+\-\d+\-\d+)$/ig;
		if(p.test(key_column_values[i])){
			var l=key_column_values.length>=15?8:5;
			key_column_values[i]=key_column_values[i].substring(l);
			continue;
		}

		var p=/^(Online for \d+d\-\d+d)$/ig;
		if(p.test(key_column_values[i])){
			var l=11;
			key_column_values[i]=key_column_values[i].substring(l);
			continue;
		}
	}

	if (reverse_key_column){
		key_column_values=key_column_values.reverse();
	}

	var xAxis={
		categories: key_column_values
	};

	var tooltip={
		formatter: function() {
				//console.log(this.x);
				//console.log(this.x.replace(/^.*?(<\/span>)/,''));
				//console.log(this.x.indexOf('</span>'));
				//console.log(this.x.length);

				return '<b>'+ this.series.name +'</b><br/>'+
				this.x +': '+ this.y ;

		}
	};

	var series=[];

	for (var i=0;i<column_names.length ;i++ ){
		var temp={};
		temp['name']=column_names[i];
		var data=[];
		var key_column_values=_get_key_column_values(table_id);
		for (var j=0; j<key_column_values.length; j++){
			var key=key_column_values[j];
			data[data.length]=_get_number(_get_cell_content(column_names[i],key,table_id));
		}
		if (reverse_key_column){
			data=data.reverse();
		}
		temp['data']=data;
		series[series.length]=temp;
	}

	var chart_param={
		chart: {
			renderTo: container_id,
			type: 'line',
			marginRight: 300,
			marginBottom: 20
		},
		title: {
			text: tab_name.replace('_',' '),
			x: -20 //center
		},
		subtitle: {
			text: 'Source: StatPortal',
			x: -20
		},
		xAxis: xAxis,
		yAxis: yAxis,
		tooltip: tooltip,
		legend: {
			layout: 'vertical',
			align: 'right',
			verticalAlign: 'top',
			x: -10,
			y: 100,
			borderWidth: 0
		},
		series: series
	};
	//console.log(series);
	//console.log(_get_key_column_values());
	//console.log(chart_param);
	//return;

	var chart = new Highcharts.Chart(chart_param);
}


//console.log(_get_column_names());

//add_highcharts_basic_line_chart_gray('Trend Comparison',_get_column_names(/Avg /ig),300);
//add_highcharts_basic_line_chart_gray('Test_Chart',['Avg Chatroom Message Created in [01d,03d]','Avg Chatroom Message Created in [04d,06d]','Avg Chatroom Message Created in [07d,09d]','Avg Chatroom Message Created in [10d,12d]','Avg Chatroom Message Created in [13d,15d]','Avg Chatroom Message Created in [16d,18d]','Avg Chatroom Message Created in [19d,21d]','Avg Chatroom Message Created in [22d,24d]','Avg Chatroom Message Created in [25d,27d]','Avg Chatroom Message Created in [28d,30d]']);

