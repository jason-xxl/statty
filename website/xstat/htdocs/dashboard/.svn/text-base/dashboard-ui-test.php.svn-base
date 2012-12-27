<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>Mozat Dashboard UI Example</title>
		
		
		<!-- 1. Add these JavaScript inclusions in the head of your page -->
		<script type="text/javascript" src="../js/jquery.js"></script>
		<script type="text/javascript" src="../js/jquery.ui.js"></script>
		<script type="text/javascript" src="./js/highcharts.js"></script>
		<link href="../css/jquery-ui.css" rel="stylesheet" type="text/css"/>
<script>

var view_content_html_cache={};

var begin_date='<?=$_GET["begin_date"]?$_GET["begin_date"]:date("Y-m-d",time()-3600*24*31)?>';
var end_date='<?=$_GET["end_date"]?$_GET["end_date"]:date("Y-m-d",time()-3600*24)?>';
var aggregation='';//'<?=$_GET["aggregation"]?>';

function dump_object(obj){
	var ret='';
	for(var i in obj){
		ret+=i+':'+obj[i]+'; '
	}
	return ret;
}

function p(s){
	//document.write(s+'<br/>');
}

function po(obj){
	//document.write(dump_object(obj)+'<br/>');
}

function get_view(view_id){
    var url_tpl='/xstat/htdocs/view.php?id={id}&date1={begin_date}&date2={end_date}&group_by={aggregation}';//&date1=2011-03-20&date2=2011-04-04';
    var view_url=url_tpl.replace('{id}',view_id)
		.replace('{aggregation}',aggregation)
		.replace('{begin_date}',begin_date)
		.replace('{end_date}',end_date);
    //p(view_url);
	if(view_content_html_cache[view_url]){
		return view_content_html_cache[view_url];
	}
	get=$.ajax({
		type:"GET",
		url:view_url,
		async:false,
		success:function(result) {
			//p(result);
			view_content_html_cache[view_url]=result.replace(/\r\n/g,'');
		}
	});
	//p(view_content_html_cache[view_url]);
	return view_content_html_cache[view_url];
}

function get_view_column_values(view_id,column_name){

    var view_content=get_view(view_id);
    if(!view_content||view_content.indexOf('<td>Password: </td>')>-1){
		location.href='/xstat/htdocs/login.php?redirect='+encodeURIComponent(location.href);
		return [];
    }
    var head_html_reg='<thead>.*?(<th>\s*'+column_name.replace('(','\\(').replace(')','\\)')+'\s*<\/th>)';
    var match=view_content.match(new RegExp(head_html_reg,'im'));
	if(!match){
		//location.href='/xstat/htdocs/login.php?redirect='+encodeURIComponent(location.href);
        return [];
	}
	match=match[0];
    match0=match.replace(/<th>/ig,'<th');
	idx=match.length-match0.length-2;
	
	var value_html_reg='<tr>.*?(?:<td.*?(?:</span>))(.*?)(?:<\/td>).*?(?:<td>.*?<\/td>){'+(idx)+'}.*?<td>(.*?)<\/td>.*?(?:<\/tr>)';

	values={};
	while(true){
		view_content=view_content.replace(new RegExp(value_html_reg,'im'),'');
		if(!RegExp.$1||!RegExp.$2){
			break;
		}
		//p(RegExp.$1);
		//p(RegExp.$2);
		values[RegExp.$1.replace(/<.*?>/ig,'').replace(/,/ig,'')]=parseInt(RegExp.$2.replace(/<.*?>/ig,'').replace(/,/ig,''));
	}
	if(values.length==0){
		alert('No Data');
	}
	return values;
}





//p(dump_object(get_view_column_values(6,'Photo UV')));
//p(dump_object(get_view_column_values(6,'Message (App) Msg Total')));
//p(dump_object(get_view_column_values(6,'Notification PV')));


function get_merged_keys(datas_dict){
	var keys_temp={};
	for(var i in datas_dict){
		var data=datas_dict[i];
		for(var i in data){
			keys_temp[i]=null;
		}
	}
	var keys=[];
	for(var i in keys_temp){
		keys[keys.length]=(i);
	}
	keys.sort();
	p(keys);
	return keys;
}

function get_merged_values(data,keys){
	values=[];
	for(var k=0;k<keys.length;k++){
		values[values.length]=data[keys[k]]||0;
	}
	p(values);
	return values;
}

function get_series(datas_dict){
	var keys=get_merged_keys(datas_dict);
	var series=[];
	for(var title in datas_dict){
		series[series.length]={
			name:title,
			data:get_merged_values(datas_dict[title],keys)
		};
	}
	p(series);
	return series;
}

function get_format_keys(keys){
	var formated_keys=[];
	var months={
		'01':'Jan',
		'02':'Feb',
		'03':'Mar',
		'04':'Apr',
		'05':'May',
		'06':'Jun',
		'07':'Jul',
		'08':'Aug',
		'09':'Sep',
		'10':'Oct',
		'11':'Nov',
		'12':'Dec'
	};
	for(var i=0;i<keys.length;i++){
		var date=keys[i].match(/^\d+\-(\d+)\-(\d+)$/);
		if(date){
			formated_keys[i]=months[RegExp.$1]+' '+RegExp.$2;
		}else{
			formated_keys[i]=keys[i];
		}
	}
	return formated_keys;
}

function draw_line_chart(datas_dict,dom_id,chart_title,chart_sub_title,yAxis_name){

	var keys=get_merged_keys(datas_dict);
	var series=get_series(datas_dict);

	//$('#'+dom_id).html('<div style="width:90%; height:20px;"></div>');

	var chart=new Highcharts.Chart({
		chart:{
			renderTo:dom_id,
			defaultSeriesType:'line',
			marginRight:130,
			marginBottom:50
		},
		title:{
			text:chart_title||'',
			x:0 //center
		},
		subtitle:{
			text:chart_sub_title||'',
			x:0
		},
		xAxis:{
			categories:get_format_keys(keys)
		},
		yAxis:{
			title:{
				text:yAxis_name||''
			},
			plotLines:[{
				value:0,
				width:1,
				color:'#808080'
			}]
		},
		tooltip:{
			formatter:function() {
				return '<b>'+ this.series.name +'</b><br/>'+
				this.x +':'+ this.y;
			}
		},
		legend:{
			layout:'vertical',
			align:'right',
			verticalAlign:'top',
			x:-10,
			y:85,
			borderWidth:0
		},
		series:series
	});
		
}

function addCommas(nStr){
	nStr += '';
	x = nStr.split('.');
	x1 = x[0];
	x2 = x.length > 1 ? '.' + x[1] : '';
	var rgx = /(\d+)(\d{3})/;
	while (rgx.test(x1)) {
		x1 = x1.replace(rgx, '$1' + ',' + '$2');
	}
	return x1 + x2;
}

function draw_pie_chart(data_dict,dom_id,chart_title,chart_sub_title,yAxis_name,top){

	//var keys=get_merged_keys(data_dict);
	//var series=get_series(data_dict);

	top=top||9;

	var series_data_tmp=[];
	for(var i in data_dict){
		series_data_tmp[series_data_tmp.length]=[i,parseFloat(data_dict[i])];
	}

	series_data_tmp.sort(function(a,b){
		return b[1]-a[1];
	});

	var series_data=[];
	for(var i=0;i<series_data_tmp.length;i++){
		if(i<top){
			series_data[i]=series_data_tmp[i];
		}else{
			if(!series_data[top]){
				series_data[top]=['Others',0.0];
			}
			series_data[top][1]+=series_data_tmp[i][1];
		}
	}

	//$('#'+dom_id).html('<div style="width:90%; height:20px;"></div>');

	var chart=new Highcharts.Chart({
		chart:{
			renderTo:dom_id,
			plotBackgroundColor:null,
			plotBorderWidth:null,
			plotShadow:false
		},
		title:{
			text:chart_title||'',
			x:0 //center
		},
		subtitle:{
			text:chart_sub_title||'',
			x:0
		},
		tooltip:{
			formatter:function() {
				return '<b>'+ this.point.name +'</b>:'+ this.y +' %';
			}
		},
		plotOptions:{
			pie:{
				allowPointSelect:true,
				cursor:'pointer',
				dataLabels:{
					enabled:false,
					color:'#000000',//Highcharts.theme.textColor || 
					connectorColor:'#000000',//Highcharts.theme.textColor || 
					formatter:function() {
						return this.y +' %';
					},
					distance:15
				},
				showInLegend:true,
				size:'88%'
			}
		},
		series:[{
			type:'pie',
			name:'Browser share',
			data:series_data
		}],
		legend: {
			align: "right",
			backgroundColor: null,
			borderColor: '#909090',
			borderRadius: 5,
			borderWidth: 1,
			enabled: true,
			floating: false,
			itemWidth: null,
			layout: "vertical",
			lineHeight: 16,
			margin: 15,
			reversed: false,
			shadow: false,
			symbolPadding: 5,
			symbolWidth: 30,
			verticalAlign: "top",
			width: 186,
			x: -25,
			y: 51
		}
	});
	var pos=$('#'+dom_id+' .highcharts-tracker').offset();
		var pos=$('html body div#container2 div#highcharts-2.highcharts-container svg g.highcharts-tracker').offset();
			return;
	$('#'+dom_id+' .highcharts-tracker').offset({
		top:pos.top,
		left:pos.left-30
	});
}

function get_trend(series_data,proportion){
	proportion=proportion||0.25;
	if(series_data.length<3){
		return 1.0;
	}
	var start=Math.floor(series_data.length*(1-0.25));
	var sum0=0,count0=0;
	for(var i=0;i<start;i++){
		sum0+=series_data[i];
		count0+=1;
	}
	var avg0=sum0/(count0||1);

	var sum1=0,count1=0;
	for(var i=start;i<series_data.length;i++){
		sum1+=series_data[i];
		count1+=1;
	}
	var avg1=sum1/(count1||1);
	return avg0==0?1:avg1/avg0;
}


function draw_table_chart(datas_dict,dom_id,chart_title,chart_sub_title){
	
	var keys=get_merged_keys(datas_dict);

	var table_content='';
	for(var i in datas_dict){
		var data_array=[];
		for(var j in keys){
			data_array[data_array.length]=datas_dict[i][keys[j]];
		}
		var trend=get_trend(data_array);
		table_content+='<tr><td class="title">'+i+'</td><td class="value">'+addCommas(datas_dict[i][keys[keys.length-1]])+'</td>'
		+'<td>'+(trend>1.0?'<img width=20 height=20 src="./img/up.png"/>':'<img width=20 height=20 src="./img/down.png"/>')+'</td></tr>';
	}

	var tpl='<div class="highcharts-container" id="highcharts-2" style="position: relative; overflow: hidden; width: 500px; height: 260px; text-align: left; font-family: \'Lucida Grande\',\'Lucida Sans Unicode\',Verdana,Arial,Helvetica,sans-serif; font-size: 12px; left: 0px; top: 0px;">				<svg xmlns="http://www.w3.org/2000/svg" version="1.1" width="500" height="260">					<defs/>					<rect rx="5" ry="5" fill="#FFFFFF" x="0" y="0" width="500" height="260" stroke-width="0" stroke="#4572A7"/>					<text x="250" y="25" style="font-family:\'Lucida Grande\', \'Lucida Sans Unicode\', Verdana, Arial, Helvetica, sans-serif;font-size:12px;color:#3E576F;font-size:16px;fill:#3E576F;" text-anchor="middle" class="highcharts-title" zindex="1">						<tspan x="250">{chart_title}</tspan>					</text>					<text x="250" y="40" style="font-family:\'Lucida Grande\', \'Lucida Sans Unicode\', Verdana, Arial, Helvetica, sans-serif;font-size:12px;color:#6D869F;fill:#6D869F;" text-anchor="middle" class="highcharts-subtitle" zindex="1">						<tspan x="250">{chart_sub_title}</tspan>					</text>				</svg>				<div class="table_container" style="position: absolute; top: 58px; left: 19px; width:462px; height:178px; z-index:1000; background-color:white; overflow: auto;">					<style>					.table_container table {width:100%; color:#222222; font-size:14px;}					.table_container td {padding:3px 16px 3px 16px;}					.table_container .title {font-weight:;}					.table_container .value {color:#555555; text-align:right;}					</style>					<table>						<tbody>{table_content}</tbody>					</table>				</div>			</div>';

	var content=tpl
		.replace('{chart_title}',chart_title)
		.replace('{chart_sub_title}',chart_sub_title)
		.replace('{table_content}',table_content);

	$('#'+dom_id).html(content);
	$('#'+dom_id+" .table_container tr:even").css({
		"background-color":"#EDEDED"
	});
    $('#'+dom_id+" .table_container tr:odd").css({
		"background-color":"#FFFFFF"
	});

}





$(document).ready(function(){

	draw_line_chart({
		'Login Unique User':get_view_column_values(691,'Mozat 6 UV'),
		'New User':get_view_column_values(692,'New Registered User (Mozat 6)')
	},'block_curve','Daily Unique User','for Mozat 6','Unique Visitor');
	
	draw_pie_chart(get_view_column_values(762,'Unique Visitor Percentage')
	,'block_country','Unique User Country Dispersion','and Ranking','Unique Visitor');
	
	draw_pie_chart(get_view_column_values(764,'Percentage')
	,'block_client','Unique User Client Type Dispersion','and Ranking','Unique Visitor');

	draw_table_chart({
		'Status Update':get_view_column_values(765,'Status Update'),
		'Photo':get_view_column_values(765,'Photo'),
		'Comment':get_view_column_values(765,'Comment'),
		'Like':get_view_column_values(765,'Like'),
		'Error Page':get_view_column_values(765,'Error Page'),
		'Mochat Msg':get_view_column_values(765,'Mochat Msg'),
		'New Facebook Account':get_view_column_values(765,'New Facebook Account'),
		'New Twitter Account':get_view_column_values(765,'New Twitter Account')
	},'block_kpi','Key Performance Indexes','and Trend');

	draw_table_chart({
		'J2me':get_view_column_values(766,'J2me User'),
		'Symbian':get_view_column_values(766,'Symbian User'),
		'Android':get_view_column_values(766,'Android User'),
		'iOS':get_view_column_values(766,'iOS User'),
		'BlackBerry':get_view_column_values(766,'BlackBerry User'),
		'WinCE':get_view_column_values(766,'WinCE User'),
		'Unknown Client Type':get_view_column_values(766,'Unknown Client Type User')
	},'block_download','Client Type Dispersion','for Mozat 6');

	$("#begin_date").datepicker({ dateFormat: 'yy-mm-dd' });
	$("#end_date").datepicker({ dateFormat: 'yy-mm-dd' });
});



</script>

	</head>
	<body style='background-color: black;'>
		
		<!-- 3. Add the container -->
		<div id="block_title" style="width:1000px; margin:5px; padding:0 0 0 5px;">	
		
			<div class="highcharts-container" id="" style="position: relative; overflow: hidden; width: 1010px; height: 50px; text-align: left; font-family: 'Lucida Grande','Lucida Sans Unicode',Verdana,Arial,Helvetica,sans-serif; font-size: 12px; left: 0px; top: 0px;">
				<svg xmlns="http://www.w3.org/2000/svg" version="1.1" width="1010" height="50">
					<defs/>
					<rect rx="5" ry="5" fill="#FFFFFF" x="0" y="0" width="1010" height="50" stroke-width="0" stroke="#4572A7"/>
				</svg>
				<div class="table_container" style="position: absolute; top: 14px; left: 10px; width:95%; height:60%; z-index:1000; background-color:white; overflow: auto;">
					<h2 style="height:60%; float:left; margin:0 0 0 15px; padding:0;">Mozat 6 Dashboard</h2>
					<div style="float:right; text-align:right;">
						<form method="GET" style="">
							<input id="begin_date" name="begin_date" type="text" value="<?=$_GET["begin_date"]?$_GET["begin_date"]:date("Y-m-d",time()-3600*24*31)?>" size="10"/>
							to
							<input id="end_date" name="end_date" type="text" value="<?=$_GET["end_date"]?$_GET["end_date"]:date("Y-m-d",time()-3600*24)?>" size="10"/>
							<select name="aggregation">
								<option value="daily">Daily</option>
								<option value="weekly">Weekly</option>
								<option value="monthly">Monthly</option>
							</select>
							<select name="country">
								<option value="(All Countries)">All Countries</option>
								<option value="United States">United States</option>
								<option value="Syrian Arab Republic">Syrian Arab Republic</option>
								<option value="Iraq">Iraq</option>
								<option value="Indonesia">Indonesia</option>
								<option value="India">India</option>
								<option value="Saudi Arabia">Saudi Arabia</option>
								<option value="Asia/Pacific Region">Asia/Pacific Region</option>
								<option value="United Kingdom">United Kingdom</option>
								<option value="Brazil">Brazil</option>
								<option value="Thailand">Thailand</option>
								<option value="Egypt">Egypt</option>
								<option value="Japan">Japan</option>
								<option value="Mexico">Mexico</option>
								<option value="Hong Kong">Hong Kong</option>
								<option value="Denmark">Denmark</option>
								<option value="Iran, Islamic Republic of">Iran, Islamic Republic of</option>
								<option value="Malaysia">Malaysia</option>
								<option value="Philippines">Philippines</option>
								<option value="Germany">Germany</option>
								<option value="Turkey">Turkey</option>
								<option value="Spain">Spain</option>
								<option value="Italy">Italy</option>
								<option value="France">France</option>
								<option value="Australia">Australia</option>
								<option value="Ireland">Ireland</option>
								<option value="Ecuador">Ecuador</option>
								<option value="Singapore">Singapore</option>
								<option value="Lebanon">Lebanon</option>
								<option value="Netherlands">Netherlands</option>
								<option value="Bangladesh">Bangladesh</option>
								<option value="Qatar">Qatar</option>
								<option value="Argentina">Argentina</option>
								<option value="China">China</option>
								<option value="Nigeria">Nigeria</option>
								<option value="Vietnam">Vietnam</option>
								<option value="Colombia">Colombia</option>
								<option value="Venezuela">Venezuela</option>
								<option value="Canada">Canada</option>
								<option value="Romania">Romania</option>
								<option value="Chile">Chile</option>
								<option value="Europe">Europe</option>
								<option value="Brunei Darussalam">Brunei Darussalam</option>
								<option value="Greece">Greece</option>
								<option value="Nepal">Nepal</option>
								<option value="Pakistan">Pakistan</option>
								<option value="Oman">Oman</option>
								<option value="Costa Rica">Costa Rica</option>
								<option value="Finland">Finland</option>
								<option value="Sweden">Sweden</option>
								<option value="Switzerland">Switzerland</option>
								<option value="Jordan">Jordan</option>
								<option value="Israel">Israel</option>
								<option value="Peru">Peru</option>
								<option value="Tunisia">Tunisia</option>
								<option value="Lao People's Democratic Republic">Lao People's Democratic Republic</option>
								<option value="Austria">Austria</option>
								<option value="Taiwan">Taiwan</option>
								<option value="United Arab Emirates">United Arab Emirates</option>
								<option value="Belgium">Belgium</option>
								<option value="Czech Republic">Czech Republic</option>
								<option value="Hungary">Hungary</option>
								<option value="Guatemala">Guatemala</option>
								<option value="Serbia">Serbia</option>
								<option value="Portugal">Portugal</option>
								<option value="Kuwait">Kuwait</option>
								<option value="Cambodia">Cambodia</option>
								<option value="Panama">Panama</option>
								<option value="Russian Federation">Russian Federation</option>
								<option value="Korea, Republic of">Korea, Republic of</option>
								<option value="Sri Lanka">Sri Lanka</option>
								<option value="Palestinian Territory">Palestinian Territory</option>
								<option value="South Africa">South Africa</option>
								<option value="Sudan">Sudan</option>
								<option value="Paraguay">Paraguay</option>
								<option value="Jamaica">Jamaica</option>
								<option value="Kenya">Kenya</option>
								<option value="Azerbaijan">Azerbaijan</option>
								<option value="Puerto Rico">Puerto Rico</option>
								<option value="Barbados">Barbados</option>
								<option value="Norway">Norway</option>
								<option value="Nicaragua">Nicaragua</option>
								<option value="El Salvador">El Salvador</option>
								<option value="Slovakia">Slovakia</option>
								<option value="Ukraine">Ukraine</option>
								<option value="Yemen">Yemen</option>
								<option value="Albania">Albania</option>
								<option value="Croatia">Croatia</option>
								<option value="Bulgaria">Bulgaria</option>
								<option value="Cyprus">Cyprus</option>
								<option value="Morocco">Morocco</option>
								<option value="New Zealand">New Zealand</option>
								<option value="Poland">Poland</option>
								<option value="Ethiopia">Ethiopia</option>
								<option value="Maldives">Maldives</option>
								<option value="Somalia">Somalia</option>
								<option value="Mozambique">Mozambique</option>
								<option value="Bolivia">Bolivia</option>
								<option value="Slovenia">Slovenia</option>
								<option value="Bahrain">Bahrain</option>
								<option value="Dominican Republic">Dominican Republic</option>
								<option value="Belarus">Belarus</option>
								<option value="Cameroon">Cameroon</option>
								<option value="Luxembourg">Luxembourg</option>
								<option value="Bosnia and Herzegovina">Bosnia and Herzegovina</option>
								<option value="Antigua and Barbuda">Antigua and Barbuda</option>
								<option value="Guyana">Guyana</option>
								<option value="Macedonia">Macedonia</option>
								<option value="Cote D'Ivoire">Cote D'Ivoire</option>
								<option value="Ghana">Ghana</option>
								<option value="Honduras">Honduras</option>
								<option value="Estonia">Estonia</option>
								<option value="Iceland">Iceland</option>
								<option value="Latvia">Latvia</option>
								<option value="Trinidad and Tobago">Trinidad and Tobago</option>
								<option value="Zimbabwe">Zimbabwe</option>
								<option value="Malta">Malta</option>
								<option value="Tanzania, United Republic of">Tanzania, United Republic of</option>
								<option value="Bahamas">Bahamas</option>
								<option value="Uruguay">Uruguay</option>
								<option value="Lithuania">Lithuania</option>
								<option value="Bhutan">Bhutan</option>
								<option value="Suriname">Suriname</option>
								<option value="Virgin Islands, U.S.">Virgin Islands, U.S.</option>
								<option value="Anonymous Proxy">Anonymous Proxy</option>
								<option value="Mongolia">Mongolia</option>
								<option value="Saint Vincent and the Grenadines">Saint Vincent and the Grenadines</option>
								<option value="Netherlands Antilles">Netherlands Antilles</option>
								<option value="Uganda">Uganda</option>
								<option value="Macau">Macau</option>
								<option value="Uzbekistan">Uzbekistan</option>
								<option value="Algeria">Algeria</option>
								<option value="Guam">Guam</option>
								<option value="Mauritius">Mauritius</option>
								<option value="Turkmenistan">Turkmenistan</option>
								<option value="Equatorial Guinea">Equatorial Guinea</option>
								<option value="Kyrgyzstan">Kyrgyzstan</option>
								<option value="Malawi">Malawi</option>
								<option value="Benin">Benin</option>
								<option value="Satellite Provider">Satellite Provider</option>
								<option value="Solomon Islands">Solomon Islands</option>
								<option value="Monaco">Monaco</option>
								<option value="Afghanistan">Afghanistan</option>
								<option value="Angola">Angola</option>
								<option value="Anguilla">Anguilla</option>
								<option value="Botswana">Botswana</option>
								<option value="Liechtenstein">Liechtenstein</option>
								<option value="Zambia">Zambia</option>
								<option value="Djibouti">Djibouti</option>
								<option value="Georgia">Georgia</option>
								<option value="Guadeloupe">Guadeloupe</option>
								<option value="Kazakstan">Kazakstan</option>
								<option value="Libyan Arab Jamahiriya">Libyan Arab Jamahiriya</option>
								<option value="Saint Lucia">Saint Lucia</option>
								<option value="Togo">Togo</option>
								<option value="Aland Islands">Aland Islands</option>
								<option value="Andorra">Andorra</option>
								<option value="Belize">Belize</option>
								<option value="Burkina Faso">Burkina Faso</option>
								<option value="Cape Verde">Cape Verde</option>
								<option value="Dominica">Dominica</option>
								<option value="Moldova, Republic of">Moldova, Republic of</option>
								<option value="Montenegro">Montenegro</option>
								<option value="Namibia">Namibia</option>
								<option value="Saint Martin">Saint Martin</option>
								<option value="Swaziland">Swaziland</option>
								<option value="(Unknown Countries)">Unknown Countries</option>
								</select>
							<input name="submit" type="submit" value="Adjust"/>
						</form>
					</div>
				</div>
			</div>
		</div>

		<div style="clear:both;"></div>
		<div id="block_curve" style="width:1000px; height:250px; margin:5px; padding:5px;"></div>
		<div id="block_kpi" style="width:490px; height:250px; margin:5px; padding:5px; float:left;"></div>
		<div id="block_client" style="width:490px; height:250px; margin:5px; padding:5px; float:left;"></div>
		<div style="clear:both;"></div>
		<div id="block_country" style="width:490px; height:250px; margin:5px; padding:5px; float:left;"></div>	
		<div id="block_download" style="width:490px; height:250px; margin:5px; padding:5px; float:left;"></div>	
		<div style="clear:both;"></div>

</body>
</html>
<!--
			<div class="highcharts-container" id="highcharts-2" style="position: relative; overflow: hidden; width: 500px; height: 260px; text-align: left; font-family: 'Lucida Grande','Lucida Sans Unicode',Verdana,Arial,Helvetica,sans-serif; font-size: 12px; left: 0px; top: 0px;">
				<svg xmlns="http://www.w3.org/2000/svg" version="1.1" width="500" height="260">
					<defs/>
					<rect rx="5" ry="5" fill="#FFFFFF" x="0" y="0" width="500" height="260" stroke-width="0" stroke="#4572A7"/>
					<text x="250" y="25" style="font-family:'Lucida Grande', 'Lucida Sans Unicode', Verdana, Arial, Helvetica, sans-serif;font-size:12px;color:#3E576F;font-size:16px;fill:#3E576F;" text-anchor="middle" class="highcharts-title" zindex="1">
						<tspan x="250">{chart_title}</tspan>
					</text>
					<text x="250" y="40" style="font-family:'Lucida Grande', 'Lucida Sans Unicode', Verdana, Arial, Helvetica, sans-serif;font-size:12px;color:#6D869F;fill:#6D869F;" text-anchor="middle" class="highcharts-subtitle" zindex="1">
						<tspan x="250">{chart_sub_title}</tspan>
					</text>
				</svg>
				<div class="table_container" style="position: absolute; top: 58px; left: 19px; width:462px; height:178px; z-index:1000; background-color:white; overflow: auto;">
					<style>
					.table_container table {width:100%; color:#222222; font-size:14px;}
					.table_container td {padding:3px 16px 3px 16px;}
					.table_container td .title {font-weight:bold;}
					.table_container td .value {color:#555555;}
					</style>
					<table>
						<tbody>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
							<tr><td class="title">Item 1-1</td><td class="value">13243546</td></tr>
						</tbody>
					</table>
				</div>
			</div>


-->