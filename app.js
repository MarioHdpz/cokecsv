var csv = require("fast-csv"),
    fs = require("fs");
var csvName = process.argv[2];
var stream = fs.createReadStream("./input/" + csvName + ".csv");
var routeCsv = csv.createWriteStream({headers: false, quoteColumns: true, quote:'"'});
var postcodesCsv = csv.createWriteStream({headers: false, quoteColumns: true, quote:'"'});
var sites = [];

routeCsv
    .pipe(fs.createWriteStream("output/routes_" + csvName +".csv"))
    .on("end", process.exit);
    
postcodesCsv
    .pipe(fs.createWriteStream("output/postcodes_" + csvName +".csv"))
    .on("end", process.exit);

csv
 .fromPath("sites.csv", {headers : true})
 .on("data", function(data){
     sites.push(data);
 })
 .on("end", function(){
     csv
      .fromStream(stream,  {headers : true})
      .on("data", function(data){
        var website = null,
           storeview = null;
           zipcode = data['Zip Code'];

        sites.forEach(function (siteData) {
          if(data.cedi_id == siteData.cedi_id) {
           website = siteData.website;
           storeview = siteData.store_view_code;
          }
        });

        if(!website || !storeview) {{
         console.log("WARNING: Can't find storeview for given cedi_id: " + data.cedi_id);
        }}

        if(zipcode.length < 5) {
          while(zipcode.length < 5) {
            zipcode = '0' + zipcode;
          }
        }
           
        routeCsv.write({
          'website': website,
          'storeview': storeview,
          'code': data['Route Code'],
          'postalcode': zipcode,
          'settlement': data.Neighborhood,
          'deliverycode': 'BOTTLER',
          'deliverydays': key(data,9),
        });
        postcodesCsv.write({
          'postcode': zipcode,
          'state': data.State,
          'municipality': data.District,
          'city': data.City,
          'type': 'Colonia',
          'settlement': data.Neighborhood
        });
      })
      .on("end", function(){
        routeCsv.end();
        postcodesCsv.end();
        console.log("Success! Let's go for some beers");
      });
 });

 var object = {
   key: function(n) {
     return this[ Object.keys(this)[n] ];
   }
 };

 function key(obj, idx) {
   return object.key.call(obj, idx);
 }