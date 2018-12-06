var request = require('request');
var parseString = require('xml2js').parseString;
var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

exports.handler = function(event, context, callback) {
    console.log(event.url); // Print the error if one occurred
    request(event.url, function (error, response, body) {
        console.log('error:', error); // Print the error if one occurred
        console.log('statusCode:', response && response.statusCode); // Print the response status code if a response was received
        if(error) callback(error);
        else{
            parseString(body, function (error, result) {
                if(error) callback(error);
                else{
                    var dbRequests = buildPutRequests(result);
                    saveItems(dbRequests);
                    callback(null, "Finished inserting");
                }
            });
        }
    });
}

function buildPutRequests(result){
    var items = [];
    for(var i = 0; i < result.kml.Document[0].Placemark.length; i++){
        items.push({
            PutRequest: {
                Item: parseItem(result.kml.Document[0].Placemark[i])
            }
        });
    }
    return items;
}

function addItem(item){
    var tableName = "chargers";
    dynamodb.putItem({
        "TableName": tableName,
        "Item" : item
    }, function(error, data) {
        if (error) {
            console.log('Error putting item into dynamodb failed: '+error);
        }
        else {
            console.log('great success: '+JSON.stringify(data, null, '  '));
        }
    });
}

function parseItem(item){
    var coordsArray = item.Point[0].coordinates[0].split(",");
    return {
        name: {S: item.name[0]},
        coordinates : { M : {
            lng: {N: coordsArray[0]},
            lat: {N: coordsArray[1]}
        }}
    }
}

function saveItems(items) {
    var chunk = 25;
    for (var i=0; i< items.length; i+=chunk) {
        console.log("SENDING:"+ i);
        var itemsChunk = items.slice(i,i+chunk);
        var params = {
            RequestItems: {
                'chargers': itemsChunk
            },
            ReturnConsumedCapacity: 'TOTAL',
            ReturnItemCollectionMetrics: 'SIZE'
        };
        dynamodb.batchWriteItem(params, function(err, data) {
            console.log("Response from DynamoDB");
            if(err) console.log(err);
            else    console.log(data);
        });
    }
}

//
// exports.handler({
//     "url": "http://www.esb.ie/ECARS/kml/charging-locations.kml"
// });