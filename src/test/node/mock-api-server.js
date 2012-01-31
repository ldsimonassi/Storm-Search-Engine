var root_dir= './src/test/resources'

var fs = require('fs');
var http = require('http');

var port = 8888;

if(process.argv.length>2)
	port = process.argv[2];
else 
	console.log("No port provided, using: " + port);

//Create the http server
http.createServer(function (request, response) {

	var file_name = root_dir + request.url;

	fs.readFile(file_name, function (err, data) {
		if (err) {
			var msg = "An error ocurred while reading file ["+file_name+"i]:"+err;
			console.log(msg);
			response.writeHead(500);
			response.end(msg);
		}
		response.writeHead(200);
		response.end(data);
	});
}).listen(port);
