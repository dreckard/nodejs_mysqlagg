// --------------------------------------------------------------------------------
// Example API endpoint for aggregating ad stats from MySQL DB to JSON
//   Database schema is given in schema.sql
// 
// Usage:
//   GET /api/stats?ad_ids=1,2,3&start_time=2013-09-01&end_time=2013-10-01
//   Results are returned in the form:
//   { '1': {
//       'impressions': 432842,  // Sum of impressions
//       'clicks': 21221,        // Sum of clicks
//       'spent': 51234,         // Sum of spent
//       'ctr': 0.0302,          // Click-through-rate
//       'cpc': 812,             // Cost per click
//       'cpm': 140,             // Cost per 1000 impressions
//       'actions': {
//         'app_install': {
//         'count': 50,        // Sum of actions
//         'value': 3300,      // Sum of action values
//         'cpa': 510          // Cost per action
//         },
//       'page_like': {
//       'count': 8,
//       'value': 0,
//       'cpa': 432 }}}
//     '2': { ... }
//   }
//
// dreckard
// March 2015
// --------------------------------------------------------------------------------
//http://localhost:3000/api/stats?ad_ids=1,2,3&start_time=2013-09-01&end_time=2013-10-01
var url = require('url');
var express = require('express');
var mysql = require('mysql');
var app = express();

var statsQry = 'SELECT ads.ad_id,SUM(impressions) impressions, ' +
                      'sum(clicks) clicks, ' +
                      'sum(spent) spent, ' +
                      'sum(clicks)/sum(impressions) ctr, ' +
                      'sum(spent)/sum(clicks) cpc, ' +
                      '1000*sum(spent)/sum(impressions) cpm, ' +
                      'ada.action,sum(ada.count) count,sum(ada.value) value, sum(spent)/sum(count) cpa ' +
               'FROM ad_statistics ads ' +
               'JOIN ad_actions ada ' +
                 'ON  ads.ad_id = ada.ad_id ' +
                 'AND ads.date = ada.date ' +
               'WHERE ads.ad_id IN (?) ' +
               '  AND ads.date BETWEEN ? AND ? ' +
               'GROUP BY ad_id,ada.action ' +
               'ORDER BY ad_id,action';

//Build compacted JSON from raw query results
function buildResultJSON(results)
{
    var output = {};
    for ( var i=0; i<results.length; i++ )
    {
        if ( output[results[i].ad_id] ) //Append action
        {
            var id_obj = output[results[i].ad_id];
            id_obj.actions[results[i].action] = { count: results[i].count, value: results[i].value, cpa: results[i].cpa };
        }
        else //New obj with first action
        {
            var id_obj = { impressions: results[i].impressions, clicks: results[i].clicks, 
                           spent: results[i].spent, ctr: results[i].ctr, cpc: results[i].cpc, cpm: results[i].cpm,
                           actions: {} };
            id_obj.actions[results[i].action] = { count: results[i].count, value: results[i].value, cpa: results[i].cpa };
            
            output[results[i].ad_id] = id_obj;
        }
    }
    return output;
}

var pool = mysql.createPool({
    connectionLimit : 500,
    host     : 'localhost',
    user     : 'root',
    password : '',
    database : 'dbname',
    debug    :  false
});

var server = app.listen(3000, function () { console.log('Server listening on port %s...', server.address().port); });

app.get('/', function (req,res) {
        res.end('Server status: ok');
    });
app.get('/api/stats', function (req,res) {
        pool.getConnection(function(err,connection){
            if (err) {
              if ( connection )
                connection.release();
              res.end('Error: Database connection failed');
              return;
            }
            var pars = url.parse(req.url,true);
            
            //The ID list needs to go in as a javascript array or the commas will be escaped and break the query
            connection.query(statsQry, [pars.query.ad_ids.split(','),pars.query.start_time,pars.query.end_time], function(err,rows) { 
                connection.release();
                if ( err ) { console.log('SQL Error: ' + err.message); return; }
                res.json(buildResultJSON(rows));
            })
        });
    });
