/**
 ********************************
 * Node.JS -> Storm DRPC Server *
 ********************************
 * 
 * This server listens in 3 ports
 * ------------------------------
 *
 * Port 8080 Synchronous task execution requests.
 * Port 8081 Pull pending tasks.
 * Port 8082 Push ready tasks results.
 *
 **/

var http = require('http');
var parser = require('url');
var os = require('os');

/**
 * Add the FIFO functionality to Array class.
 **/
Array.prototype.store = function (info) {
  this.push(info);
}

Array.prototype.fetch = function() {
  if (this.length <= 0) { return ''; }  // nothing in array
  return this.shift(); 
}

Array.prototype.display = function() {
  return this.join('\n');
}

// Metrics
var total_active_tasks = 0;
var total_requests_made = 0;
var total_requests_answered = 0;

// Tasks FIFO
var pending_tasks = new Array();

// Waiting workers FIFO
var waiting_workers = new Array();

// Current active tasks (Assigned to a worker).
var active_tasks = {};

// This RPC Server Request ID
var global_task_id = 0;

// My IP, to be sent in the origin
var local_ip= null;

function get_local_ip() {
	if(local_ip==null) {
		var interfaces= os.networkInterfaces();
		for(var interf_name in interfaces) {
			var addresses= interfaces[interf_name];
			for(var addr_name in addresses) {
				var addr= addresses[addr_name];
				if(addr.family=="IPv4" && !addr.internal && (/en\d/.test(interf_name) || /eth\d/.test(interf_name))) {
					local_ip= addr.address;
					return local_ip;
				}	
			}
		}
	}
	return local_ip;
}

// If there is a task for a worker, make them meet each other!
function check_queues() {
	if(waiting_workers.length > 0 && pending_tasks.length > 0) {
		var worker =	waiting_workers.fetch();
		var max= worker.max;
		var send = get_local_ip() +"\n";

		for(var i=0; (i<max) && (pending_tasks.length > 0);i++){
			var task = pending_tasks.fetch();
			send = send + task.id +"\n";
			send = send + task.query +"\n";
			active_tasks[task.id] =  task;
			total_active_tasks = total_active_tasks + 1;
		}
		worker.response.end(send);
	}
}

// Server to be used to receive search querys (tasks) and answer them in a synchronous way.
http.createServer(function (request, response) {
	var query= request.url;
	if(query=="/isAlive")
		response.end("YES!");
	else {
		var task_entry = { "id":global_task_id, "query": query, "response": response };
		global_task_id = global_task_id+1;
		pending_tasks.store(task_entry);
		total_requests_made++;
		check_queues();
	}
}).listen(8080);

// Server to be used for requesting pending tasks
http.createServer(function (request, response) {
	var parsed_url= parser.parse(request.url, true);
	var max= parsed_url.query.max;
	if(max==null || typeof(max)=="undefined")
		max=1;
	var waiter = { "request": request, "response": response, "max": max };
	waiting_workers.store(waiter);
	check_queues();
}).listen(8081);

// Response receiver server
http.createServer(function (request, response) {
	if(request.method=="POST") {
		var parsed_url= parser.parse(request.url, true);
		var id= parsed_url.query.id;
		if(id==null || typeof(id)=="undefined" || 
		   active_tasks[id]==null || typeof(active_tasks[id])=="undefined") {
			response.writeHead(404);
			response.end("Error ["+id+"] is not a waiting task in this server");
		} else {
			var data='';
			request.on("data", function(chunk) {
				data += chunk;
			});
		
			request.on("end", function() {
				active_tasks[id].response.end(data);
				delete active_tasks[id]
				total_active_tasks = total_active_tasks - 1;
				response.end("OK!\n");
				total_requests_answered++;
			});
		}
	}
}).listen(8082);

// Log status information each 10 seconds interval.
setInterval(function () {
	var d= new Date();
	console.log("****************************");
	console.log("* Date: "+ d.getFullYear() +"/"+(d.getMonth()+1)+"/"+d.getDate()+" "+d.getHours()+":"+d.getMinutes()+":"+d.getSeconds());
	console.log("* Local IP: "+local_ip);
	console.log("* Total requests made: "+ total_requests_made);
	console.log("* Total requests answered: "+  total_requests_answered);
	console.log("* Waiting workers: "+ waiting_workers.length);
	console.log("* Pending tasks: "+  pending_tasks.length);
	console.log("* Active threads: "+ total_active_tasks);
	for(var task in active_tasks) {
		console.log(task);
	}
	console.log("****************************");
}, 10000) ;