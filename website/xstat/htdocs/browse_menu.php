<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<html>
<head>
    <title> Menu </title>
    <meta name="Generator" content="EditPlus">
    <meta name="Author" content="">
    <meta name="Keywords" content="">
    <meta name="Description" content="">

    <link rel="stylesheet" href="./css/jquery-ui.css" type="text/css" media="all" />
    <link rel="stylesheet" href="./analyz/css/jquery.treeview.css" />

    <script src="./analyz/js/jquery.min.js" type="text/javascript"></script>
    <script src="./analyz/js/jquery-ui.min.js" type="text/javascript"></script>
    <script src="./analyz/js/jquery.cookie.js" type="text/javascript"></script>
    <script src="./analyz/js/jquery.treeview.js" type="text/javascript"></script>
    <script src="./analyz/js/jquery.blockui.js" type="text/javascript"></script>

    <script>

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

    function dump(e){
        var s='';
        for(var k in e){
            s+=k+'|';
        }
        alert(s);
    }

    $(function(){
		/*
        $("#browse a").bind('click',function(e){
            var href=e.getAttribute('href');
            if(url){
                parent.content.location=url;
            }
            return false;
        });
		*/

        $("#browse").treeview({
            animated: "fast",
            collapsed: true,
            unique: true,
            persist: "cookie",
            toggle: function(){
                window.console && console.log("%o was toggled", this);
            }
        });
    });

    </script>
    <style>
    
    body {
        font-size: 12px;
    }
    #browse a {
        padding-left: 5px;
        cursor: pointer;
        cursor: hand;
        color: gray;
        font-weight: normal;
    }
    #browse span {
        padding-left: 5px;
        cursor: pointer;
        cursor: hand;
        color: gray;
        font-weight: normal;
    }
    #browse a:hover {
        color: black;
        font-weight: normal;
    }
    </style>

</head>

<body>

<h3>Browse</h3>

<ul id="browse" class="tree">

    <li><span class="folder">Over Operator</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=93">Operator Comparison</a></li>
        </ul>
    </li>

    <li><span class="folder">STC</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=102">User Growth</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=94">IM</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=95">Social Network</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=96">Game</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=97">Application</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=104">Service Quality</a></li>
            <!--<li><a class="file" target="content" href="./user_view.php#tabs-Project">Project</a></li>-->
        </ul>
    </li>

    <li><span class="folder">Telk_Armor</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=139">User Growth</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=135">IM</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=136">Social Network</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=137">Game</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=138">Application</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=140">Service Quality</a></li>
            <!--<li><a class="file" target="content" href="./user_view.php#tabs-Project">Project</a></li>-->
        </ul>
    </li>

    <li><span class="folder">Viva</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=132">User Growth</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=128">IM</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=129">Social Network</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=130">Game</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=131">Application</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=133">Service Quality</a></li>
            <!--<li><a class="file" target="content" href="./user_view.php#tabs-Project">Project</a></li>-->
        </ul>
    </li>

    <li><span class="folder">Viva_BH</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=153">User Growth</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=149">IM</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=150">Social Network</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=151">Game</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=152">Application</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=154">Service Quality</a></li>
            <!--<li><a class="file" target="content" href="./user_view.php#tabs-Project">Project</a></li>-->
        </ul>
    </li>

    <li><span class="folder">Umniah</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=125">User Growth</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=121">IM</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=122">Social Network</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=123">Game</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=124">Application</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=126">Service Quality</a></li>
            <!--<li><a class="file" target="content" href="./user_view.php#tabs-Project">Project</a></li>-->
        </ul>
    </li>
    
    <li><span class="folder">Mozat</span>
        <ul>
            <li><a class="file" target="content" href="./page_v.php?id=173">Mozat 6</a></li>
    <!--
            <li><a class="file" target="content" href="./page_v.php?id=146">User Growth</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=142">IM</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=143">Social Network</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=144">Game</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=145">Application</a></li>
            <li><a class="file" target="content" href="./page_v.php?id=147">Service Quality</a></li>
    -->
        </ul>
    </li>

</ul>


</body>

</html>
