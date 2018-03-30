#!/usr/bin/env node
var knox = require('knox')
var split2 = require('split2')
var once = require('once')
var mergesort = require('mergesort-stream')
var t2 = require('through2')

var argv = require('yargs')
  .option('bucket', {
    alias: 'b',
    description: 'the s3 bucket to read',
    required:true,
  })
  .option('key', {
    alias: 'k',
    description: 'the s3 key to use. defaults to env AWS_ACCESS_KEY ',
    default: process.env.AWS_ACCESS_KEY,
    required:true,
  })
  .option('start',{
    alias:'s',
    description:"the time to start from. A Date.parse compatible time string.",
    required:true,
  })
  .option('aws-secret', {
    description: 'the s3 secret to use. defaults to env AWS_SECRET_KEY ',
    default: process.env.AWS_SECRET_KEY,
    required:true,
  })
  .option('end',{
    alias:'e',
    description:"the time to stop. defaults to 1 minute after start.",
    default:false,
  })
  .option('offsets',{
    description:"instead of streaming logs only print the log files and the offset of the time in the logs."
  })
  .argv


var client = knox.createClient({
      key: argv.key|| process.env.AWS_ACCESS_KEY
      , secret: argv.secret|| process.env.AWS_SECRET_KEY
      , bucket: argv.bucket
});

var endWindow = 0;//3*60*1000;
var startWindow = 5*1000;

// logs in this formaT.
// s3://bucket/2017-04-19T20:00:00.000-Me-EgChM68AAAAA.log

time = Date.parse(argv.start)

// default 1 minute start at ^
timeEnd = argv.end?Date.parse(argv.end):time + (1*60*1000);

findTime(time,function(err,logs){
  if(err) throw err;

  if(argv.offsets){
    console.log(logs)
    return;
  }
  
  offset = Math.floor((1000*60*5)/2)

  // object stream of lines.
  streamLogs(logs,time,timeEnd,function(err,stream){
    if(err) {
      throw err
    }

    stream.pipe(process.stdout)
  });

})


function findTime(time,cb){

  hour = time - (time % (1000*60*60))
  hour = new Date(hour).toJSON()

  if(!hour) {
    return cb(new Error('accepts one argument the time + or - 3 minutes of log data to stream. '+time))
  }

  hour = hour.substr(0,hour.length-10)

  logs(hour,function(err,data){
    if(err) return console.log('error listing bucket '+err);

    if(!data.Contents || !data.Contents.length){
      return console.log('couldnt find logs ',data);
    }

    var obj = data.Contents[0];

    var ranges = {}
    var c = data.Contents.length

    data.Contents.forEach(function(obj){
      search(obj.Key,obj.Size,time-startWindow,time,function(err,start){
        if(err) console.error("error loading "+obj.Key+': '+err);
        else ranges[obj.Key] = {start:start,size:obj.Size};

        if(!--c){
          cb(false,ranges);
        }
      })
    })
  })
}

function logs(hour,cb){

  client.list({prefix:hour},function(err,data){
    cb(err,data)
  })
}

var ID = 0

function search(file,size,start,end,cb){

  var _id = ++ID;

  var chunkSize = 600;
  var position = Math.floor(size/2)
  var loops = 0

  if(!cb) throw new Error('missing callback')

  cb = once(cb)

  var max = size;
  var min = 0;

  ;(function loop(){
    if(position >= size){
      return cb(new Error('could not find time near end of log'))
    }

    if(position < 0){
      return cb(new Error('could not find time near start of log'))
    }

    if(++loops >= 40) {

      return cb(new Error('loops never stopped '+loops))
    }

    //console.log('getting range: ',file,position,position+chunkSize)
    getRange(file,position,position+chunkSize,function(err,res){
      lines = [];
      var checked = false;

      var startPosition = position;

      //console.log(_id+'position>',position,max,min)

      res.pipe(split2()).on('data',function(l){
        lines.push(l)
        if(lines.length != 2) {
          return;
        }

        checked = true;
        var ts = logTime(l)
        //console.log(l)
        if(ts < start){
          // cannot be less than position.
          min = position

          position += Math.ceil((max-min)/2)

          if(position > max){
            throw new Error(max+'-'+min+'/2 = '+Math.floor((max-min)/2)+' which is < than '+max)
          }

          if(position >= size){
            cb(new Error('not found'))
          } else {
            loop()
          }
        } else if(ts > end){
          max = position
          position -= Math.floor((max-min)/2)

          if(position < min){
            throw new Error(max+'-'+min+'/2 = '+Math.floor((max-min)/2)+' which is > than '+min)
          }

          if(position <= 0){
            cb(new Error('not found')) 
          } else {
            loop()
          }
        } else {
          // between start and end.
          // stream chunks ${direction} till we find the start of the range.
          cb(false,position,size,loops);
        }

        res.destroy()

      }).on('end',function(){
        //console.log(_id+' s:e',startPosition === position,' lines: '+lines.length);
        if(lines.length <= 2){
          chunkSize += chunkSize;
          if(chunkSize > 1000000) {
            chunkSize = 1000000; 
          }

          if(!checked) {
            loop()
          }
        }
      })
    })
  }())
  
}

function streamLogs(logData,startTime,endTime,cb) {
  cb = once(cb);
  var streams = []
  var keys = Object.keys(logData);
  var c = keys.length;

  var ended = false;
  keys.forEach(function(file){
    var data = logData[file]
    getRange(file,data.start,data.size,function(err,res){
      if(err || ended){
        ended = true;
        if(res) streams.push(res);
        while(streams.length) streams.shift().destroy();
        return cb(err);
      }
      streams.push(res)
      if(!--c){
        pipeAndFilter(streams)
      }
    })
  })

  function pipeAndFilter(streams){  
    var splits = []
    streams.forEach(function(s){
      splits.push(s.pipe(split2()))
    })

    var sorted = mergesort(function(line1,line2){
      var t1 = line1.time || logTime(line1)
      var t2 = line2.time || logTime(line2)
      line1.time = t1
      line2.time = t2
      if(t1 > t2) return 1
      else if(t1 < t2) return -1
      return 0
    },splits)

    // write sorted logs out
    var ended = false;
    sorted.pipe(t2(function(l,enc,cb){
      var time = logTime(l)
      if(time < startTime){
        //return cb()
      }

      if(time > endTime && !ended){
        ended = true;
        streams.forEach(function(s){
          s.destroy()
        })
        // ending. eat errors.
        sorted.on('error',function(){})
          
        sorted.end()
        return;
      }

      if(!ended) cb(false,l+'\n')
    })).pipe(process.stdout)
  }
}

function getRange(file,start,end,cb){
  cb = once(cb)
  client.get(file,{
    Range:'bytes='+start+'-'+end 
  }).on('response',function(res){
    cb(false,res)
  }).on('error',function(){
    cb(err) 
  }).end()
}

function logTime(l){
  var timestr = l.slice(l.indexOf('>')+1,l.indexOf(' '))
  return Date.parse(timestr)
}
