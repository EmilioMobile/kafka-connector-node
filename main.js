var Twitter = require('twitter');
var config = require('./config.js');
var express = require('express');
var kafka = require('kafka-node');
var uuid = require('uuid');
var app = express();

// Set up your search parameters
var params = {
  q: '#nodejs',
  count: 10,
  result_type: 'recent',
  lang: 'en'
}

var bodyParser = require('body-parser');
app.use( bodyParser.json() );       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));

const client = new kafka.Client("ec2-52-78-61-52.ap-northeast-2.compute.amazonaws.com", "my-client-id", {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
});

var Producer = kafka.Producer,
  producer = new Producer(client);

producer.on('ready', function () {
  console.log('Producer is ready');
});

producer.on('error', function (err) {
  console.log('Producer is in error state');
  console.log(err);
})

app.get('/',function(req,res){
  res.json({greeting:'Kafka Consumer'})
});

/* Kafka Payloads */
/*{
   topic: 'topicName',
   messages: ['message body'], // multi messages should be a array, single message can be just a string or a KeyedMessage instance
   key: 'theKey', // only needed when using keyed partitioner (optional)
   partition: 0, // default 0 (optional)
   attributes: 2 // default: 0 used for compression (optional)
}*/

app.post('/sendMsg',function(req,res){
    var sentMessage = JSON.stringify(req.body.message);
    console.log(sentMessage);
    payloads = [
        { topic: req.body.topic, messages:sentMessage , partition: 0 }
    ];
    producer.send(payloads, function (err, data) {
            res.json(data);
    });
})

app.post('/sendTweets', function(req,res){
  var T = new Twitter(config);
  T.get('search/tweets', params, function(err, data, response) {
    if(!err){
      // This is where the magic will happen
      // Loop through the returned tweets
      for(let i = 0; i < data.statuses.length; i++){
        // Get the tweet Id from the returned data
        let id = { id: data.statuses[i].id_str }
        // Try to Favorite the selected Tweet
        T.post('favorites/create', id, function(err, response){
          // If the favorite fails, log the error message
          if(err){
            console.log(err[0].message);
          }
          // If the favorite is successful, log the url of the tweet
          else{
            let username = response.user.screen_name;
            let tweetId = response.id_str;
            console.log('Favorited: ', `https://twitter.com/${username}/status/${tweetId}`)
          }
        });
      }
    } else {
      console.log(err);
    }
  })
}


app.listen(5001,function(){
  console.log('Kafka producer running at 5001')
});
