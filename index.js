var express = require('express'),
    cfenv = require('cfenv'),
    request = require('request'),
    async = require('async');

var ENV = process.env,
    conductor_api = ENV.CONDCTOR_API,
    workflow_name = ENV.WORKFLOW_NAME,
    task_name = ENV.TASK_NAME,
    worker_id = ENV.WORKER_ID, //unused for now
    poll_rate = ENV.POLL_RATE || 30,
    time_interval = poll_rate * 1000,
    is_runnning = false;

// create a new express server
var app = express();

// get the app environment from Cloud Foundry
var appEnv = cfenv.getAppEnv();


//Following flow: https://github.com/Netflix/conductor/issues/9

function pollForTask(callback){
    console.log('In pollForTask');
    var headers = {
      'headers': {
        'Accept': 'application/json'
      }
    }

    request.get(conductor_api + '/tasks/poll/batch/' + task_name + '?timeout=100', headers, (req, res) => {
      var body, workflowInstanceId, taskId, inputData = null;
      body = JSON.parse(res.body)[0];

      if(res.statusCode != 200 || !body){
        return callback('[' + res.statusCode + '] Task not found');
      }

      workflowInstanceId = body.workflowInstanceId;
      taskId = body.taskId;
      inputData = body.inputData;


      callback(null, taskId, workflowInstanceId, inputData);
    });
}

function ackTask(task_id, workflow_instance_id, input_data, callback){
  console.log('In ackTask');
  var headers = {
    'headers': {
      'Content-Type': '*/*',
      'Accept': 'text/plain'
    }
  }
  request.post(conductor_api + '/tasks/' + task_id + '/ack?workerId=' + worker_id, headers, (req, res) => {
    var body = res.body;

    if(res.statusCode != 200 || body != 'true'){
      return callback('[' + res.statusCode + '] Failed to ack');
    }

    callback(null, task_id, workflow_instance_id, input_data)
    //updateTaskStatus(task_id, workflow_instance_id, 'COMPLETED')

  });
}

function processTask(task_id, workflow_instance_id, input_data, callback){
    console.log('In processTask');

    //Do work in here

    taskStatus = 'COMPLETED';
    callback(null, task_id, workflow_instance_id, taskStatus);
}

function updateTaskStatus(task_id, workflow_instance_id, task_status, callback){
    console.log('in updateTaskStatus');

    var headers = {
      'headers': {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      'json': {
        'workflowInstanceId': workflow_instance_id,
        'taskId': task_id,
        'status': task_status
      }
    };

    request.post(conductor_api + '/tasks', headers, (req, res) => {
      var body = res.body;
      //console.log(body);
      if(res.statusCode != 204){
        return callback('[' + res.statusCode + '] Failed to update status');
      }
      callback();

    });
}


// Basic task flow
function waterfallTasks(){
  console.log('Running waterfall');
  is_running = true;
  async.waterfall([
    pollForTask,
    ackTask,
    processTask,
    updateTaskStatus
    ], function(error){
      if(error){
        console.log('Error: ' + error);
      }
      console.log('Finished runnning, waiting (' + poll_rate + ' seconds) for next task');
      is_running = false;
  });
}

// Run task every time_interval if it is not running
setInterval(function() {
  if(!is_running){
    waterfallTasks();
  }
}, time_interval);

// First waterfall run on startup
waterfallTasks();

/** -------------------------------------------------
 * Express stuff, remove if not using monitoring
 ------------------------------------------------- **/
//Health check endpoint
app.get('/', (req, res) => {
  res.send('ok');
});


// start server on the specified port and binding host
app.listen(appEnv.port, '0.0.0.0', function() {
  // print a message when the server starts listening
  console.log('server starting on ' + appEnv.url);
});




