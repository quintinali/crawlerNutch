var g_currentQuery, g_totalCount, g_bLoading = false, g_hasMore = true, g_resultCount, g_tableObj;
var RESULT_LIMIT = 100;

$(document).ready(function() {
	var query = getURLParameter('query');
	g_currentQuery = encodeURI(query);
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
	
	g_tableObj = $('#ResultsTable').DataTable({
		"sPaginationType": "full_numbers",
		"bSort": false,
		"pageLength": 10,
		"dom": 'tp',
		"language": {
			"emptyTable": "No entry matches!"
		},
		"fnDrawCallback": function ( oSettings ) {
		    $(oSettings.nTHead).hide();
		    $(oSettings.nTFoot).hide();
		}
	}).on('page.dt', function () {
	    var info = g_tableObj.page.info();
	    if(info.end == info.recordsTotal && g_hasMore && !g_bLoading) {
	    	//load more
	    	g_bLoading = true;
	    	$.when(req_search(g_currentQuery)).done(function (searchResult) {
	    		if (searchResult != null) {
	    			var searchResults = searchResult.SearchResults;
	    			if(searchResult.ResultCount < RESULT_LIMIT) {
	    				g_hasMore = false;
	    			}
	    			if (searchResults.length > 0) {
	    				g_resultCount += searchResults.length;
	    				searchResults = processTableDataSource(searchResults);
	    				for(var i = 0; i < searchResults.length; i++) {
	    					g_tableObj.row.add([tableRowFormatter(searchResults[i])]);
	    				}
	    				g_tableObj.draw(false);
	    			}
    				updateResultCountLabel();
	    		}
	    		g_bLoading = false;
	        }).fail(function (jqXHR, textStatus) {
	            alert("Error: " + jqXHR.statusText, 'error');
	    		g_bLoading = false;
	        });
	    }
	});
	$(".dataTables_empty").hide();
	
	$('#ResultsTable').on("click", ".more", function(){
		window.open($(this).parents("td").first().find("h4 a").attr("href"));
	});
});

function search(query) {
	//reset counters
	g_totalCount = g_resultCount = 0;
	g_searchResult = [];
	g_hasMore = true;
	g_currentQuery = $("#query").val();
	
	$.when(req_search(g_currentQuery)).done(function (searchResult) {
		$(".dataTables_empty").show();
		if (searchResult != null) {
			g_totalCount = searchResult.ResultCount;
			var searchResults = searchResult.SearchResults;
			g_resultCount = searchResults.length;
			if(g_resultCount < RESULT_LIMIT) {
				g_hasMore = false;
			} 
			if (g_resultCount > 0) {
				searchResults = processTableDataSource(searchResults);
				for(var i = 0; i < g_resultCount; i++) {
					g_tableObj.row.add([tableRowFormatter(searchResults[i])]);
				}
				g_tableObj.draw(false);
			}
			updateResultCountLabel();
		}
    }).fail(function (jqXHR, textStatus) {
        alert("Error: " + jqXHR.statusText, 'error');
    });
}

function req_search(search) {
	return $.ajax({
		url : "SearchByQuery",
		data : {
			"query" : search,
			"result_from": g_resultCount,
			"result_limit": RESULT_LIMIT
		}
	});
}

function processTableDataSource(dataSource) {
	if(null == dataSource || 0 == dataSource.length) return dataSource;
	for(var i = 0; i < dataSource.length; i++) {
		if(dataSource[i].summary.length > 500) {
			dataSource[i].Content = '<label class="shortenContent">' + dataSource[i].summary.substring(0, 500) + '...' + '</label><label class="fullContent">' + dataSource[i].Content + '</label><span class="more">[More]</span>';
		}else{
			dataSource[i].Content = '<label class="shortenContent">' + dataSource[i].summary + '</label><label class="fullContent">' + dataSource[i].Content + '</label><span class="more">[More]</span>';
		}
			}
	return dataSource;
}

function updateResultCountLabel() {
	if(0 == g_resultCount) {
		$("#resultCount").html('No entry matches!');
	} else if(g_tableObj.page.info().recordsTotal == g_tableObj.page.info().recordsDisplay) {
		$("#resultCount").html('Showing first ' + g_resultCount + ' entries (from  ' + g_totalCount + ' matched entries)');
	} else {
		$("#resultCount").html('Showing first ' + g_tableObj.page.info().recordsDisplay + ' of ' + g_resultCount + ' entries (from  ' + g_totalCount + ' matched entries)');
	}
}

function FileNameFormatter(value, url) {
	if(url)
	{
	   return '<h4><a href=' + url + ' target="_blank" class="resultContent">' + value + '</a></h4>'; 
	}
	else{
		url = "FileUpload?fileName="+encodeURIComponent(value);	
		return '<h4><a href=' + url + ' target="_blank" class="resultContent">' + value + '</a></h4>'; 
	}
}

function URLFormatter(URL, org) {
	if(!org) return '';
	return '<h5 class="text-success resultContent"><a href="' + URL + '">' + URL + '</a></h5>'; 
	//return '<img class="searchResultFavicon" src="http://' + extractDomain(URL) + '/favicon.ico" onError="$(this).remove();return false;"/>' + '<h5 class="text-success resultContent"><a href="http://' + extractDomain(URL) + '">' + org + '</a></h5>'; 
}

function DefaultFormatter(value) {
	return '<h5 class="resultContent">' + value + '</h5>'; 
}

function tableRowFormatter(searchResult) {
	if(!searchResult) return '';
	var row = "";
	row += '<div class="card-view"><span class="value">' + FileNameFormatter(searchResult.Title, searchResult.URL) + '</span></div>';
	row += '<div class="card-view" style="line-height: 0.9;"><span class="value">' + URLFormatter(searchResult.URL, searchResult.URL) + '</span></div>';
	//row += '<div class="card-view"><span class="value">' + URLFormatter(searchResult.URL, searchResult.Organization) + '</span></div>';
	//row += '<div class="card-view"><span class="value"><i>keywords</i>: ' +  searchResult.keywords + '</span></div>';
	//row += '<div class="card-view"><span class="value"><i>chunker keywords</i>: ' +  searchResult.chunkerKeyword+ '</span></div>';
	//row += '<div class="card-view"><span class="value"><i>gold keywords</i>: ' +  searchResult.goldKeywords+ '</span></div>';
	if(typeof(searchResult.allKeywords) != "undefined"){
		row += '<div class="card-view"><span class="value"><i>keywords</i>: ' +  searchResult.allKeywords + '</span></div>';
	}
	//row += '<div class="card-view"><span class="value"><i>score</i>: ' +  searchResult.score + '</span></div>';
	row += '<div class="card-view"><span class="value">' + DefaultFormatter(searchResult.Content) + '</span></div>';

	return row;
}

function applyFilter() {
	var filter = $("#query").val();
	g_tableObj.search(filter).draw();
	updateResultCountLabel();
}

function extractDomain(url) {
	if(!url) return url;
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