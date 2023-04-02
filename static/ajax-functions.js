// Load this automatically - parse get parameters to object called 'get_params'
var get_params = {}
location.search.substr(1).split("&").forEach(function(item) {
  get_params[item.split("=")[0]] = item.split("=")[1];
}); 

const MakeAjaxCall = async (url, options) => {

  if (options == undefined)
    options = {method: 'GET', mode: 'cors', cache: 'no-cache'};

  const ajax_request = new Request(url, options);
  const ajax_response = await fetch(ajax_request);

  if (ajax_response.ok) {

    const json_data = await ajax_response.json();
    return Promise.resolve(json_data);

  } else {

    return Promise.reject('Ajax call failed:' + ajax_request);

  }
}

