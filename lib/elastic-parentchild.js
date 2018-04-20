/**
 * Created by ananyagoel on 16/04/18.
 */


var _ = require('lodash');
var elasticsearch = require("elasticsearch");

//es_db includes parent and child config
//data is data from parent



function add_child(url,es_db,data, filter) {
    var client;
    var counter;
    var parent_id_array;
    var total_count;
    var parent_id_search_object;

    client =  new elasticsearch.Client({
        host: url,
        requestTimeout: Infinity
    });
    if(typeof(filter) === 'undefined'){
        filter=[]
    }
    counter =0;
    total_count =0;
    parent_id_array = _.map(es_db, "_id");
    parent_id_search_object =
        {
        has_parent: {
            parent_type: es_db.parent_type,
            query:{
                ids:{
                    values: parent_id_array
                }
            }
        }
    }

    filter.push(parent_id_search_object);

    client.search({
        index: es_db.index,
        type: es_db.child_type,
        body: {
            query: {
                bool: {
                    must:filter
                }
            }
        }
    }, function getMoreUntilDone(error, result) {
        var response_array = [];
        counter =0;
        if (error) {
            console.error(error);
        } else {
            _.map(data, function (obj) {
                counter++;
                total_count++;
                _.assign(obj.child_array, _.find(result, {_parent: obj._id}));
            if(counter === result.hits.hits.length) {
                if (result.hits.total > total_count) {
                    client.scroll({
                        scrollId: result._scroll_id,
                        scroll: '5m',
                        size: 10000
                    }, getMoreUntilDone);
                } else {
                    console.log("completed!");
                    return data;

                }
            }

            });
        }
    });

}

module.exports={
    add_child : add_child
}