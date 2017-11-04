var csv = require("fast-csv"),
    fs = require("fs");
var csvName = process.argv[2];
var stream = fs.createReadStream("./input/" + csvName + ".csv");
var routeCsv = csv.format({headers: true, quoteColumns: true});
var postcodesCsv = csv.format({headers: true, quoteColumns: true});
var sites = [];

routeCsv
    .pipe(fs.createWriteStream("output/routes_" + csvName +".csv"), {headers: true, quoteColumns: true})
    .on("end", process.exit);
    
postcodesCsv
    .pipe(fs.createWriteStream("output/postcodes_" + csvName +".csv"), {headers: true, quoteColumns: true})
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
        
          routeCsv.write([
            ['website','store','storeview','postalcode','deliveryco','deliverydays','status','settlement','code'], 
            [website, website, storeview, data['Zip Code'], 'BOTTLER', data['Delivery days'], '1', data.Neighborhood, data['Route Code']]
          ]);
          postcodesCsv.write([
            ['postcode','settlement','type','municipality','state','city'], 
            [data['Zip Code'], data.Neighborhood, 'Colonia', data.District, data.State, data.City]
          ]);
          console.log(data);
      })
      .on("end", function(){
         routeCsv.end();
         console.log("done");
      });
 });

