var g_currentQuery, g_totalCount, g_searchResult = [], g_bLoading = false, g_scrollID, g_resultCount;
var RESULT_LIMIT = 200;

$(document).ready(function() {
	var query = getURLParameter('query');
	g_currentQuery = encodeURI(query);
	$("#searchResults").hide();
	$("#NotFound").hide();
	$("#query").val(query);
	search(query);

	$("#query").keyup(function(event) {
		if (event.keyCode == 13) {
			$("#searchButton").click();
		}
	});

	$("#searchButton").click(function() {
		redirect("index", "query", $("#query").val());
	});
	
	var win = $(".master-overlay-main");
	// browser window scroll (in pixels) after which the "back to top" link is shown
	var offset = 300,
		//browser window scroll (in pixels) after which the "back to top" link opacity is reduced
		offset_opacity = 1200,
		//duration of the top scrolling animation (in ms)
		scroll_top_duration = 700,
		//grab the "back to top" link
		$back_to_top = $('.cd-top');

	// Each time the user scrolls
	win.scroll(function() {
		//hide or show the "back to top" link
			( $(this).scrollTop() > offset ) ? $back_to_top.addClass('cd-is-visible') : $back_to_top.removeClass('cd-is-visible cd-fade-out');
			if( $(this).scrollTop() > offset_opacity ) { 
				$back_to_top.addClass('cd-fade-out');
			}
			
		// End of the document reached?
		if ($("#ResultsTable").height() - win.height() <= win.scrollTop() + 100 && !g_bLoading && !$("#filter").val()) {
			g_bLoading = true;
			$('#loadingMore').show();

			$.ajax({
				url : "SearchByQuery",
				data : {
					"query" : $("#query").val(),
					"result_from": g_scrollID,
					"result_limit": RESULT_LIMIT
				},
				success : function completeHandler(response) {
					g_bLoading = false;
					$('#loadingMore').hide();
					if (response != null) {
						var searchResults = response.SearchResults;
						g_scrollID = response.ScrollID;
						
						if (searchResults.length == 0) {
//							$("#NotFound").show();
//							$("#searchKeyword").html($("#query").val());
//							$("#resultCount, #ontology-results").hide();
						} else {
							g_searchResult = g_searchResult.concat(searchResults);
							g_resultCount += searchResults.length;
							updateResultCountLabel();
							//$("#resultCount").html('Showing ' + g_resultCount + ' results of ' + g_totalCount + ' matches');
							$('#ResultsTable').bootstrapTable('append', processTableDataSource(searchResults));
						}
					}
				}
			});
		}
	});
	
	//smooth scroll to top
	$back_to_top.on('click', function(event){
		event.preventDefault();
		$('.master-overlay-main').animate({
			scrollTop: 0 ,
		 	}, scroll_top_duration
		);
	});
});

function search(query) {
	//if ($("#query").val() != "") {
		$("#searchBox").append($("#searchGroup"));
		$("#searchjumbo").hide();
		$("#note").hide();
		$("#searchResults").show();
		$("#searchLoading").show();

		$("#searchContainer").css("margin-top", "30px");
		$("#searchResultContainer").show();
		$("#searchContainer h2.title").css("font-size", "24px");
		$.ajax({
			url : "SearchByQuery",
			data : {
				"query" : $("#query").val(),
				"result_from": "",
				"result_limit": RESULT_LIMIT
			},
			success : function completeHandler(response) {
				if (response != null) {
					$("#searchLoading").hide();
					g_totalCount = response.ResultCount;
					g_scrollID = response.ScrollID;
					g_searchResult = response.SearchResults;
					if (g_searchResult.length == 0 || response.ResultCount <= 0) {
						$("#NotFound").show();
						$("#searchKeyword").html($("#query").val());
						updateResultCountLabel();
					} else {
						g_resultCount = g_searchResult.length;
						$("#NotFound").hide();
						updateResultCountLabel();
						createResultTable();
						$('#ResultsTable').bootstrapTable('load', processTableDataSource(g_searchResult));
					}
				}
			}
		});
	//}
}

function processTableDataSource(dataSource) {
	var tableDataSource = [];
	for(var i = 0; i < dataSource.length; i++) {
		var searchResult = jQuery.extend({}, dataSource[i]);
		if(searchResult.Content.length > 500) {
			searchResult.Content = searchResult.Content.substring(0, 500) + '...';
		}
		
		tableDataSource.push(searchResult);
	}
	return tableDataSource;
}

function updateResultCountLabel(filterCount) {
	if(null == filterCount) {
		$("#resultCount").html('Showing ' + g_resultCount + ' entries (filtered from  ' + g_totalCount + ' total entries)');
	} else {
		$("#resultCount").html('Showing ' + filterCount + ' of ' + g_resultCount + ' entries (filtered from  ' + g_totalCount + ' total entries)');
	}
}

function FileNameFormatter(value, row) {
	if(row.Type == "webpage")
	{
	   var weburl = row.URL;
	   return '<h4><a href=' + weburl + ' target="_blank" class="resultContent">' + value + '</a></h4>'; 
	}
	else{
		var url = "FileUpload?fileName="+encodeURIComponent(value);	
		return '<h4><a href=' + url + ' target="_blank" class="resultContent">' + value + '</a></h4>'; 
	}
}

function URLFormatter(value, row) {
	return '<h5 class="text-success resultContent">' + extractDomain(value) + '</h5>'; 
}

function TimeFormatter(value, row) {
	return '<h5 style="font-style: italic" class="resultContent">' + value + '</h5>'; 
}

function DefaultFormatter(value, row) {
	return '<h5 class="resultContent">' + value + '</h5>'; 
}

function createResultTable() {
	var layout = {
		cache : false,
		pagination : false,
		striped : true,
		cardView : true,
		showHeader : false,

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
			'title' : 'Content',
			'field' : 'Content',
			'formatter' : DefaultFormatter,
		}
		]
	};

	$('#ResultsTable').bootstrapTable(layout);
}

function applyFilter() {
	var filter = $("#query").val();
	$("#ResultsTable tbody tr").show();
	updateResultCountLabel();
	if(filter) {
		var count = g_searchResult.length;
		for(var i = 0; i < g_searchResult.length; i++) {
			if(!contains(g_searchResult[i].Content, filter)){
				$("#ResultsTable tbody tr:nth-child(" + (i + 1) + ")").hide();
				count--;
			}
		}

		updateResultCountLabel(count);
	}
}

function contains(str, searchStr) {
	if(!str) return false;
	if(!searchStr) return true;
	return str.toLowerCase().indexOf(searchStr.toLowerCase()) >= 0;
}

function extractDomain(url) {
    var domain;
    //find & remove protocol (http, ftp, etc.) and get domain
    if (url.indexOf("://") > -1) {
        domain = url.split('/')[2];
    }
    else {
        domain = url.split('/')[0];
    }

    //find & remove port number
    domain = domain.split(':')[0];

    return domain.replace('www.', '');
}

var typewatch = function(){
    var timer = 0;
    return function(callback, ms){
        clearTimeout (timer);
        timer = setTimeout(callback, ms);
    }  
}();