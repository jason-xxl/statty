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

<h3>User Management</h3>

<ul id="browse" class="tree">

    <li><span class="folder"><a class="file" target="content" href="group.php">Groups</a></span></li>
    <li><span class="folder"><a class="file" target="content" href="user.php">Users</a></span></li>

</ul>

</body>

</html>
