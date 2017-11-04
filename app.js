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
     console.log("Sites read");
     csv
      .fromStream(stream,  {headers : true})
      .on("data", function(data){
           var website = null,
               storeview = null;
           
           sites.forEach(function (siteData) {
             if(data.cedi_id == siteData.cedi_id) {
               website = siteData.website;
               storeview = siteData.store_view_code;
             }
           });
           
           if(!website || !storeview) {{
             console.log("WARNING: Can't find storeview forgiven cedi_id: ".data.cedi_id);
           }}
        
          routeCsv.write({
            'website': website,
            //'store': website,
            'storeview': storeview,
            'postalcode': data['Zip Code'],
            'settlement': data.Neighborhood,
            'deliverycode': 'BOTTLER',
            'deliverydays': data['Delivery days'],
            //'status': '1',
            //'code': data['Route Code'] 
          }
        );
          postcodesCsv.write({
            'postcode': data['Zip Code'],
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

