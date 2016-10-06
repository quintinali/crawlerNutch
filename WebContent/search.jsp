<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="sec" uri="http://www.springframework.org/security/tags" %>
<%@page session="true"%>
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Planetary Defense</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="images/NASALogo_burned.png">
  
  <link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.0/jquery.min.js"></script>
  <script src="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js"></script>
  <!-- bt jasny -->
  <link rel="stylesheet" href="http://cdnjs.cloudflare.com/ajax/libs/jasny-bootstrap/3.1.3/css/jasny-bootstrap.min.css">
  <script src="http://cdnjs.cloudflare.com/ajax/libs/jasny-bootstrap/3.1.3/js/jasny-bootstrap.min.js"></script>
  
  <!-- bt table -->
	<link rel="stylesheet" href="http://cdnjs.cloudflare.com/ajax/libs/bootstrap-table/1.10.1/bootstrap-table.min.css">
	<script src="http://cdnjs.cloudflare.com/ajax/libs/bootstrap-table/1.10.1/bootstrap-table.min.js"></script>

  <script src="//cdnjs.cloudflare.com/ajax/libs/jqueryui/1.11.2/jquery-ui.js"></script>
  
  <!-- Custom styles for this template -->
  <link href="css/dashboard.css" rel="stylesheet">
  <script src="js/util.js"></script>
</head>

<script type="text/javascript">
	$(document).ready(function(){
	$("#query").autocomplete({
            source: function(request, response) {
                $.ajax({
                    url: "AutoComplete",
                    type: "POST",
                    data: {chars: $("#query").val()},

                    dataType: "json",

                    success: function(data) {
					    response(data);
                    }
               });              
            }   
        });
	
		var query = getURLParameter('query');
		var filter = getURLParameter('fileType');
		
		if(query==null)
		{			
			$("#searchResults").hide();
		}else{
			$("#searchResults").hide();
			$("#NotFound").hide();
			$("#query").val(query);
			search(query, filter, 'fileType');			
		}

		$("#query").keyup(function(event){
			if(event.keyCode == 13){
				$("#searchButton").click();
			}
		});		
		
		$("#searchButton").click(function() {				
			//setGetParameter("query", $("#query").val());
			var urlParams = 
				{
					"query": $("#query").val()
				};

			setGetParameters(urlParams);
	   });

	});
	
	function search(query, filter_val, filter_field){
	if($("#query").val()!="")
				{								
				$("#searchBox").append($("#searchGroup"));
				$("#searchjumbo").hide();
				$("#note").hide();
				$("#searchResults").show();
				$("#searchLoading").show();
				$('#ResultsTable').bootstrapTable('destroy');
				$("#facetPanels").empty();
				$.ajax({
					url : "SearchByQuery",
					data : {
								"query" : $("#query").val(),
								"filter": filter_val,
								"filter_field": filter_field
						   },
					success : function completeHandler(response) {
						if(response!=null)
						{
							$("#searchLoading").hide();
							console.log(response);
							var searchResults = response.SearchResults;
							if(searchResults.length==0)
							{
							$("#NotFound").show();
							}else{						
							createResultTable();
							$('#ResultsTable').bootstrapTable('load', searchResults);
							
							createFacetPanel(response.FacetResults);
							}
						}					
					}
				});		
		
			   }
	}
	
	function FileNameFormatter(value, row) {
		if(row.Type == "webpage")
		{
		   var weburl = row.URL;
		   return '<h4><a href=' + weburl + ' target="_blank">' + value + '</a></h4>'; 
		}
		else{
			var url = "FileUpload?fileName="+encodeURIComponent(value);	
			return '<h4><a href=' + url + ' target="_blank">' + value + '</a></h4>'; 
		}
    }
	
	function URLFormatter(value, row) {
		return '<h5 class="text-success">' + value + '</h5>'; 
    }
	
	function TimeFormatter(value, row) {
		return '<h5 style="font-style: italic">' + value + '</h5>'; 
    }
	
	function DefaultFormatter(value, row) {
		return '<h5>' + value + '</h5>'; 
    }
	
	function createFacetPanel(FacetResults)
	{
		var col_div = $("#facetPanels");
	   for (var property in FacetResults) {
			if (FacetResults.hasOwnProperty(property)) {
			    var list = FacetResults[property];
				//for(var i=0; i<list.length; i++)
				//   {
					var panel_div = $("<div/>", {
											class : "panel panel-default",
											}).appendTo(col_div);
													
					var heading_div = $("<div/>", {
											class : "panel-heading",
											html: "<h4 class=\"panel-title\"><a data-toggle=\"collapse\" href=\"#collapse1\">"+property+"<\/a><\/h4>"
													}).appendTo(panel_div);
					var body_class = "panel-collapse collapse in";	
                    /*if(i != 0)
					{
						body_class = "panel-collapse collapse";	
					}*/
					
					var body_div = $("<div/>", {
											class : body_class,
											id: property
												}).appendTo(panel_div);
												
					fillPanelBody(body_div, list, property);
				//	}
			}
		} 							   
	}
		
	function fillPanelBody(body_div, list, property)
	{
		 var ul_div = $("<ul/>", {
									class : "list-group",
							}).appendTo(body_div);
		 for(var i=0; i<list.length; i++)
		 {
			var ul_div = $("<li/>", {
									class : "list-group-item",
									html:"<a>"+list[i].Key+"<\/a><span class=\"pull-right badge\">"+list[i].Value+"<\/span>"
							}).appendTo(body_div);
			ul_div.click({param1: list[i].Key, param2: property}, function(event) {
			  var query = getURLParameter('query');
			  //extract parameters from URL when searching and pass to search function
			  //search(query, event.data.param1, event.data.param2);	

			  var urlParams = 
				{
					"fileType": event.data.param1
				};

			   setGetParameters(urlParams);


			  
			});
		 }
	}
	
	function createResultTable() {
		var layout = {
			cache : false,
			pagination : true,
			pageSize : 10,
			striped: true,
			//pageList : [ 11, 25, 50, 100, 200 ],
			//sortName : "Time",
			//sortOrder : "asc",
			cardView: true,
			showHeader: false,

			columns : [ {
				'title' : 'Title',
				'field' : 'Title',
				'formatter' : FileNameFormatter,
				sortable : true
			}, 
			{
				'title' : 'URL',
				'field' : 'URL',
				'formatter' : URLFormatter,
			},
			{
				'title' : 'Time',
				'field' : 'Time',
				'formatter' : TimeFormatter,
			}, 
			/*{			
				'title' : 'Type',
				'field' : 'Type',
			},*/
			{
				'title' : 'Content',
				'field' : 'Content',
				'formatter' : DefaultFormatter,
			}
			]

		};

		$('#ResultsTable').bootstrapTable(layout);
	}
	
	
</script>

<body>
    <div id = "alert_placeholder" style="width:80%;margin: 0 auto"></div>
    <nav class="navbar navbar-inverse navbar-fixed-top">
      <div class="container-fluid">
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
		  <a href="javascript:;" class="pull-left"><img src="images/NASALogo_burned.png" height="50">
          <a href="javascript:;" class="navbar-brand">Document Exchange and Archive Portal for Planetary Defense</a>
        </div>
		
		<ul class="nav navbar-nav navbar-right">
        <!--<li><a href="javascript:;">Welcome, <sec:authentication property="principal.username" /></a></li>-->
        <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false" style="font-size:medium" id = "welUsername">
		  Welcome, <sec:authentication property="principal.username" />
		  <span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="javascript:;" id = "changepwd">Change password</a></li>
            <li role="separator" class="divider"></li>
            <li><a href="logout"><strong> Logout</strong></a></li>
          </ul>
        </li>
      </ul>
		
		<!--<form class="navbar-form navbar-right">
            <div class="form-group">
              <input type="text" placeholder="Email" class="form-control">
            </div>
            <div class="form-group">
              <input type="password" placeholder="Password" class="form-control">
            </div>
            <button type="submit" class="btn btn-default">Sign in</button>
        </form>-->
		
      </div>
    </nav>
	
	<div class="container-fluid">
      <div class="row">
        <div class="col-sm-3 col-md-2 sidebar">
          <ul class="nav nav-sidebar">
            <li><a href="doc.jsp">Document archiving</a></li>
			<li class="active"><a id= "searchNav" href = "search.jsp">Content search</a></li>			
          </ul>
		  
		  <ul class="nav nav-sidebar">
            <li><a href="http://199.26.254.186/pdwiki/index.php/Main_Page" target="_blank">Wiki</a></li>
          </ul>
          
        </div>
        <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
          <h1 class="page-header">Content search</h1>
		  <p class="lead" id = "note">This search engine currently supports most of the common document formats, as well as image and video formats. For a comprehensive list of supported
		  formats, please visit
		  <a href="https://tika.apache.org/1.12/formats.html" target="_blank">Apache Tika</a> project.</p>


		<div id= "searchjumbo" class="container" style = "width:84%;margin:7%">
	      <h2 style ="text-align:center">Planetary Defense Documents Discovery <img src="images/docs.png" height="45"></h2>
		  <p style ="text-align:center">Please search for archived documents by keywords.</p>
		  <div class="input-group" style="width:56%;;margin:0 auto" id = "searchGroup">
			   <input type="text" class="form-control" placeholder="Search text..." id="query" name="query" value="">
			   <div class="input-group-btn">
				   <button type="button" class="btn btn-success" id = "searchButton" ><span class="glyphicon glyphicon-search"></span></button>
				</div>
		  </div>

		  <p style="margin-left:22%;margin-top:1%"><a class="btn btn-primary" href="doc.jsp"><span class="glyphicon glyphicon-eye-open"></span> Browse latest documents</a></p>
	   </div>
	   
	   <div class="container" id = "searchResults" style="width:90%">
	     <div class="row" style = "border-bottom:1px solid #ddd; padding-bottom:10px;margin-bottom:10px;position:relative">
		   <div class="col-md-12" id = "searchBox">
		   </div>
         </div> 
		 <p id = "searchLoading" style="text-align:center; margin:10%"><img src="images/loading.gif" height="80"><br> Please wait while results are loading. </p>
         
		<div class="row" id = "resultPanels"> 
         <div class="col-md-3" id="facetPanels">	
		 
			<!--<div class="panel panel-default">
			  <div class="panel-heading">
				<h4 class="panel-title">
				  <a data-toggle="collapse" href="#collapse1">Collapsible list group</a>
				</h4>
			  </div>
			  <div id="collapse1" class="panel-collapse collapse in">
				<ul class="list-group">
				  <li class="list-group-item"><span>One</span><span class="pull-right badge">123</span></li>
				  <li class="list-group-item">Two</li>
				  <li class="list-group-item">Three</li>
				</ul> 				     
			  </div>
	        </div> -->

         </div>	
         <div class="col-md-9">	
		 <table id="ResultsTable" class="table"></table>
         </div>			 
         	 
        </div> 
		 
		 <div class="row" id = "NotFound" style = "font-size:medium">
			Your search did not match any documents. <br><strong>Suggestions</strong>:
			<ul>
			  <li>Check spelling.</li>
			  <li>Try different keywords.</li>
			  <li>Try fewer and more general keywords.</li>
			</ul> 
         </div>
		 	
	   </div>

        </div>	
      </div>
	  
    </div>
	
	<div class="modal fade" id="pwdModal" tabindex="-1" role="dialog">
			  <div class="modal-dialog" role="document">
				<div class="modal-content">
				  <div class="modal-header">
					<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
					<h4 class="modal-title">Change password</h4>
				  </div>
				  <div class="modal-body">
					<form>
					  <div class="form-group">
						<label for="concept-name" class="control-label">Current password:</label>
						<input type="password" class="form-control" id="oldpwd">
					  </div>
					  <div class="form-group">
						<label for="concept-name" class="control-label">New password:</label>
						<input type="password" class="form-control" id="newpwd">
						<span id="pwdBlock" class="help-block"></span>
					  </div>
					</form>
				  </div>
				  <div class="modal-footer">
					<button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
					<button type="button" class="btn btn-primary" id = "changePwdSubmit">Submit</button>
				  </div>
				</div>
			  </div>
	</div>
	
	<footer class="footer">
      <div class="row" style = "margin:0">
		  <div class="col-md-2">
		  </div>
		  <div class="col-md-10">
		  <div class="container-fluid" style = "text-align:center;padding-top:20px;padding-bottom:5px;font-size:small">
		 This system is funded by NASA <a href="https://www.nasa.gov/goddard">Goddard</a> and <a href="http://www.nsf.gov/">NSF</a> (NNG16PU001), and supported by NASA <a href="https://esto.nasa.gov/info_technologies_aist.html">AIST</a> (NNX15AM85G). Developed and hosted by 
		<a href="http://stcenter.net/stc/">NSF Spatiotemporal Innovation Center</a> on the <a href="http://cloud.gmu.edu">Hybrid Cloud Service</a>.
         
		  </div>
		  </div>
			
		  </div>
    </footer>
</body>
</html>