var request = require('request');
var parseString = require('xml2js').parseString;
var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

var styleURLToType = {
    StandardType2 : "Standard Type 2",
    Own_StandardType2 : "Standard Type 2",
    Own_StandardType2_Part : "Standard Type 2",
    Own_StandardType2_OOS : "Standard Type 2",
    Own_StandardType2_Occ : "Standard Type 2",
    Own_StandardType2_right : "Standard Type 2",
    Own_StandardType2_right_Occ : "Standard Type 2",
    Other_StandardType2 : "Standard Type 2",
    Other_StandardType2_OOS : "Standard Type 2",
    Other_StandardType2_right : "Standard Type 2",
    Other_StandardType2_Occ : "Standard Type 2",
    Other_StandardType2_Part : "Standard Type 2",
    Other_StandardType2_right_Occ : "Standard Type 2",
    Other_Hotel : "Other",
    Other_Services : "Other",
    Own_FastAC43 : "Fast AC",
    Own_FastAC43_Occ : "Fast AC",
    Own_FastAC43_left : "Fast AC",
    Own_FastAC43_OOS : "Fast AC",
    Own_FastAC43_left_OOS : "Fast AC",
    Own_FastAC43_left_Occ : "Fast AC",
    Other_FastAC43_Occ : "Fast AC",
    Other_FastAC43 : "Fast AC",
    Other_FastAC43_left : "Fast AC",
    Other_FastAC43_OOS : "Fast AC",
    Own_ComboCCS : "Combo CCS",
    Own_ComboCCS_OOS : "Combo CCS",
    Own_ComboCCS_Occ : "Combo CCS",
    Own_ComboCCS_left : "Combo CCS",
    Own_ComboCCS_left_Occ : "Combo CCS",
    Own_ComboCCS_left_Occ : "Combo CCS",
    Other_ComboCCS : "Combo CCS",
    Other_ComboCCS_OOS : "Combo CCS",
    Other_ComboCCS_Occ : "Combo CCS",
    Other_ComboCCS_left_Occ : "Combo CCS",
    Other_CHAdeMO : "Chademo",
    Other_CHAdeMO_left : "Chademo",
    Other_CHAdeMO_left_Occ : "Chademo",
    Other_CHAdeMO_Occ : "Chademo",
    Other_CHAdeMO_left_OOS : "Chademo",
    Own_CHAdeMO : "Chademo",
    Own_CHAdeMO_left : "Chademo",
    Own_CHAdeMO_Occ : "Chademo",
    Own_CHAdeMO_OOS : "Chademo",
    Own_CHAdeMO_OOS : "Chademo",
    Own_CHAdeMO_left_Occ : "Chademo",
    Own_CHAdeMO_left_OOS : "Chademo"
}

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

function parseItem(item){
    var coordsArray = item.Point[0].coordinates[0].split(",");
    var type = styleURLToType[item.styleUrl[0].substr(1)];
    if(!styleURLToType.hasOwnProperty(item.styleUrl[0].substr(1))){
        console.log(item.styleUrl[0].substr(1));
    }
    return {
        name: {S: item.name[0]},
        type: {S: type},
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