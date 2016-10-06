function showAlert(holderID, message, alertType) {		
		$('#' + holderID).append('<div id="alertdiv" class="alert alert-' +  alertType.toLowerCase() + ' alert-fixed-top' + 
		'"><a class="close" data-dismiss="alert">&times;</a><strong>' + alertType + '! </strong><span>'+message+'</span></div>');
		
		$("#alertdiv").css("text-align", "center");
		
		setTimeout(function() { 
		  $("#alertdiv").remove();
		}, 5000);
}

$(document).ready(function(){
	        $("#changepwd").click(function() {				
				$('#pwdModal').modal('show');
		    });
			
			$("#changePwdSubmit").click(function() {
                if($("#oldpwd").val()!="" && $("#newpwd").val()!="")
				{
				    if($("#oldpwd").val()!= $("#newpwd").val())
					{
					$("#newpwdForm").removeClass("has-error");
				    $('#pwdBlock').text("");
					$('#pwdModal').modal('hide');				
					$.ajax({
						url : "Changepwd",
						data : {
									"username" : $("#welUsername").text().trim().substring(9),
									"oldpwd": $("#oldpwd").val(),
									"newpwd": $("#newpwd").val()
							   },
						success : function completeHandler(response) {
								if(response.exception!=null)
								{
                                showAlert("alert_placeholder", response.exception, "Danger");
								}else{
								showAlert("alert_placeholder", response.PDResults, "Success");
								window.location.href = "logout";
								}
						}
					});			  
					}else{
					   $("#newpwdForm").addClass("has-error");
					   $('#pwdBlock').text('Your new password is the same as your current one.');
					}
				}				
		    });
});

/*function setGetParameter(paramName, paramValue)
{
	var url = window.location.href; //URL of the current page
	var hash = location.hash; //anchor part of the URL after '#'
	url = url.replace(hash, ''); //replaces anchor part of URL with ''

	if (url.indexOf(paramName + "=") >= 0) //contains paramName + "="
	{
		var prefix = url.substring(0, url.indexOf(paramName)); //goes from beginning to start of paramName
		var suffix = url.substring(url.indexOf(paramName)); //goes from start of paramName to the end

		//suffix = suffix.substring(suffix.indexOf("=") + 1); 
		//suffix = (suffix.indexOf("&") >= 0) ? suffix.substring(suffix.indexOf("&")) : ""; 

		if(suffix.indexOf("&") >= 0) //if suffix has "&" in it
		{
			suffix = suffix.substring(suffix.indexOf("&")) //change suffix to start of & till end
		}
		else //if suffix does not have "&" in it
		{
			suffix = ""; //change suffix to nothing
		}

		url = prefix + paramName + "=" + paramValue + suffix;
	}
	else //does not contain paramName + "="
	{
		if (url.indexOf("?") < 0) //if URL contains no "?"
		{
			url += "?" + paramName + "=" + paramValue;
		}
		else //URL must contain "?"
		{
			url += "&" + paramName + "=" + paramValue;
		}
	}
	window.location.href = url + hash;  //url changes - browser reloads page
} */

function setGetParameters(obj)
{
	var url = window.location.href;
	var hash = location.hash;
	url = url.replace(hash, '');

	/*for(var key in paramNames)
	{
		if (!paramNames.hasOwnProperty(key)) continue;
		var obj = paramNames[key]; */  //object within object
		for(var prop in obj) //iterate through objects in the object
		{
			if(!obj.hasOwnProperty(prop)) continue;
			if (url.indexOf(prop + "=") >= 0) //contains prop + "="
			{
				var prefix = url.substring(0, url.indexOf(prop)); //goes from beginning to start of prop
				var suffix = url.substring(url.indexOf(prop)); //goes from start of prop to the end

				if(suffix.indexOf("&") >= 0) //if suffix has "&" in it
				{
					suffix = suffix.substring(suffix.indexOf("&")) //change suffix to start of & till end
				}
				else //if suffix does not have "&" in it
				{
					suffix = ""; //change suffix to nothing
				}

				url = prefix + prop + "=" + obj[prop] + suffix;
			}
			else //does not contain prop + "="
			{
				if (url.indexOf("?") < 0) //if URL contains no "?"
				{
					url += "?" + prop + "=" + obj[prop];
				}
				else //URL must contain "?"
				{
					url += "&" + prop + "=" + obj[prop];
				}
			}
			window.location.href = url + hash;  //url changes - browser reloads page
		}
	//}
	
}

function getURLParameter(name) {
	return decodeURIComponent((new RegExp('[?|&]' + name + '=' + '([^&;]+?)(&|#|;|$)').exec(location.search)||[,""])[1].replace(/\+/g, '%20'))||null;
}

function getParas() {   //http://stackoverflow.com/questions/901115/how-can-i-get-query-string-values-in-javascript
    var match,
        pl     = /\+/g,  // Regex for replacing addition symbol with a space
        search = /([^&=]+)=?([^&]*)/g,
        decode = function (s) { return decodeURIComponent(s.replace(pl, " ")); },
        query  = "?i=main&mode=front&sid=de8d49b78a85a322c4155015fdce22c4&enc=+Hello%20&empty";

    var urlParams = {};
    while (match = search.exec(query))
       urlParams[decode(match[1])] = decode(match[2]);
	   
	return urlParams;
}