//Victoria Dataset Conversion Project
//Created by Daniel Tucker

const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csv = require('csv-parser');

//declares objects
let sites = [{}];
let samples = [{}];
let newSites = [{}];
let newSamples = [{}];
let siteData = [];
let sampleData = []; 
let dupes = {};
let latitude = {}; 
let longitude = {}; 

//object for csv format that will be output for sites
const csvWriterSites = createCsvWriter({
    path: ('Sites.csv'),
    header: [
        {id: 'Site_ID', title: 'Site ID'},
        {id: 'Site_Name', title: 'Site Name'},
        {id: 'Site_Type', title: 'Site Type'},
        {id: 'Sample_Matrix_Type', title: 'Sample Matrix Type'},
        {id: 'Institution_Type', title: 'Institution Type'},
        {id: 'Admin0_(Country/Region/Sovereignty)', title: 'Admin0 (Country/Region/Sovereignty)'},
        {id: 'Admin1_(Province/State/Dependency)', title: 'Admin1 (Province/State/Dependency)'},
        {id: 'Admin2_(County/Commune/Municipality)', title: 'Admin2 (County/Commune/Municipality)'},
        {id: 'Admin3_(City/City Area)', title: 'Admin3 (City/City Area)'},
        {id: 'Address_Line', title: 'Address Line'},
        {id: 'Postal_Code', title: 'Postal Code'},
        {id: 'latitude', title: 'Latitude'},
        {id: 'longitude', title: 'Longitude'},
        {id: 'Geodata_Link', title: 'Geodata Link'},
        {id: 'Contact_Person', title: 'Contact Person'},
        {id: 'phone', title: 'Phone'},
        {id: 'email', title: 'Email'},
        {id: 'Population_Served', title: 'Population Served'},
        {id: 'Annual_Average_Daily_Flow', title: 'Annual Average Daily Flow'},
        {id: 'Flow_Units', title: 'Flow Units'},
        {id: 'Sampling_Frequency', title: 'Sampling Frequency'},
        {id: 'active', title: 'Active'}
    ]
});


//object for csv format that will be output for samples
const csvWriterSamples = createCsvWriter({
    path: ('Samples.csv'),
    header: [
        {id: 'Sample_ID', title: 'Sample ID'},
        {id: 'Lab_Method_ID', title: 'Lab Method ID'},
        {id: 'Site_ID', title: 'Site ID'},
        {id: 'Date/Time', title: 'Date/Time'},
        {id: 'Sample_Collection_Method', title: 'Sample Collection Method'},
        {id: 'Target_1', title: 'Target 1'},
        {id: 'Target_1_Type', title: 'Target 1 Type'},
        {id: 'Target_1_Result', title: 'Target 1 Result'},
        {id: 'Target_1_Concentration', title: 'Target 1 Concentration'},
        {id: 'Target_1_Units', title: 'Target 1 Units'},
        {id: 'Target_1_Detection Limit', title: 'Target 1 Detection Limit'},
        {id: 'Target_2', title: 'Target 2'},
        {id: 'Target_2_Type', title: 'Target 2 Type'},
        {id: 'Target_2_Result', title: 'Target 2 Result'},
        {id: 'Target_2_Concentration', title: 'Target 2 Concentration'},
        {id: 'Target_2_Units', title: 'Target 2 Units'},
        {id: 'Target_2_Detection Limit', title: 'Target 2 Detection Limit'},
        {id: 'Passed_QA_QC', title: 'Passed QA/QC'},
        {id: 'flow', title: 'Flow'},
        {id: 'Flow_Units', title: 'Flow Units'},
        {id: 'ph', title: 'pH'},
        {id: 'tss', title: 'TSS'},
        {id: 'conductivity', title: 'Conductivity'},
        {id: 'Water_Temperature', title: 'Water Temperature'},
        {id: 'notes', title: 'Notes'}
    ]
});


//reads csv file by row
fs.createReadStream('wastewater_levels_public.csv')
    .pipe(csv())
    .on('data', (rows) => {
        siteData.push(rows);
    }).on('end', () => {

        //filters out any duplicate sites by its id
        const result = siteData.filter(function(currentObject) {
            if (currentObject.SiteId in dupes) {
                return false;
            } else {
                dupes[currentObject.SiteId] = true;
                return true;
            }
        })

        //important for-loop that allows data to be written into the Sites csv object
        for(var i = 0; i < result.length; i++) {

            //assigns the id to each site by saving it to temp variable
            const id = 'AUS_Vic_Site';
            const num = i + 1;
            const stringNum = num.toString()
            const newString = id.concat(stringNum);

            //saves the site name into a temporary variable
            const siteName = result[i].SiteName;

            //saves the latitude and longitude to temporary variables
            //takes into account if either are missing or are in the wrong order
            let lat;
            let long;
            latitude[i] = result[i].centroidLat;
            longitude[i] = result[i].centroidLong;

            if(latitude[i] == ' ' || longitude[i] == ' '){
                lat = ' '; 
                long = ' ';
            }
            else{
                if(latitude[i] > 0 || longitude[i] < 0){
                    lat = longitude[i].toString();
                    long = latitude[i].toString();
                }
                else{
                    lat = latitude[i].toString(); 
                    long = longitude[i].toString();
                }
            }

            //pushes the saved data to the Sites csv object that will be output
            sites.push({
                'Site_ID': newString,
                'Site_Name': siteName,
                'Site_Type': 'influent at treatment plant',
                'Sample_Matrix_Type': 'Wastewater',
                'Institution_Type': ' ',
                'Admin0_(Country/Region/Sovereignty)': 'Australia',
                'Admin1_(Province/State/Dependency)': 'Victoria',
                'Admin2_(County/Commune/Municipality)': ' ',
                'Admin3_(City/City Area)': ' ',
                'Address_Line': ' ',
                'Postal_Code': ' ',
                'latitude': lat,
                'longitude': long,
                'Geodata_Link': ' ',
                'Contact_Person': ' ',
                'phone': ' ',
                'email': ' ',
                'Population_Served': ' ',
                'Annual_Average_Daily_Flow': ' ',
                'Flow_Units': ' ',
                'Sampling_Frequency': ' ',
                'active': ' '
            });

            //parses the Sites data from the old csv object to a new object
            //this circumvents the issue of the site object outputting a csv file with the first row empty
            for(var j = 1; j < sites.length; j++){
                newSites[j-1] = sites[j];
            }

            //outputs the parsed Sites data as an csv file
            csvWriterSites.writeRecords(newSites);
            
        }
    });

    //reads csv file by row
    fs.createReadStream('wastewater_levels_public.csv')
        .pipe(csv())
        .on('data', (rows) => {
            sampleData.push(rows);
        }).on('end', () => {

        //declares temperary arrays
        var tempName = [];
        var tempDetect = []; 
        var tempDate = [];

        //couldnt directly read from sampleData (i dont know why) when trying to compare an iteration (sampleData[i].SitName == sampleData[i+1].SitName), so I chose to store them into temp storage array. 
        //stores the name of the sites, if covid was detected, and the date of the sample test 
        for(var i = 0; i < sampleData.length; i++){
            tempName[i] = sampleData[i].SiteName;
        }
        for(var ii = 0; ii < sampleData.length; ii++){
            tempDetect[ii] = sampleData[ii].SampleResult; 
        }
        for(var iii = 0; iii < sampleData.length; iii++){
            tempDate[iii] = sampleData[iii].SampleCollectionDateStart; 
        }

        //declared variables used to label each site and sample with their own id 
        var fullSampleId;
        const siteId = 'AUS_Vic_Site';
        const sampleId = '_S'
        var newString;
        var newString2; 
        var k = 1; 
        var m = 1; 

        //important for-loop that allows data to be written into the Samples csv object
        for(var j = 0; j < sampleData.length; j++){

            //else-if statements that allows the sample Id and site Id to be created at the same time 
            if(tempName[j] == tempName[j+1]){

                //(1) creates site id 
                const num = k;
                const stringNum = num.toString()
                newString = siteId.concat(stringNum);

                //(2) creates half of the sample id
                const num2 = m; 
                const stringNum2 = num2.toString();
                var newString2 = sampleId.concat(stringNum2);

                //(3) combines the two previous string to create the sample id 
                fullSampleId = newString.concat(newString2);
                m++;
            }
            else if(tempName[j] != tempName[j+1]){

                //same as (1)
                const num = k;
                const stringNum = num.toString()
                newString = siteId.concat(stringNum);
                k++;

                //same as (2)
                const num2 = m; 
                const stringNum2 = num2.toString();
                var newString2 = sampleId.concat(stringNum2);

                //same as (3)
                fullSampleId = newString.concat(newString2);
                m = 1;
            }

            //determines if the csv file should have presence or absence if detected or not detected was stated
            var detection; 
            if(tempDetect[j] == 'Detected'){
                detection = 'presence';
            }
            else if(tempDetect[j] == 'Not Detected'){
                detection = 'absence ';
            }
            else{
                detection = ' ';
            }

            //block of code that changes the date format from the original file to the required W-SPHERE formatted date 
            var sampleDate = tempDate[j];
            var modifiedDate = sampleDate.split(' ')[0];
            var month = modifiedDate.split('/')[1];
            var year = modifiedDate.split('/').pop();
            var day = modifiedDate.split('/')[0];
            var formattedDate = year + '-' + month + '-' + day; 

            //pushes the saved data to the Samples csv object that will be output
            samples.push({
                'Sample_ID': fullSampleId,
                'Lab_Method_ID': 'AUS_VIC_Lab1',
                'Site_ID': newString,
                'Date/Time': formattedDate,
                'Sample_Collection_Method': ' ',
                'Target_1': ' ',
                'Target_1_Type': ' ',
                'Target_1_Result': detection,
                'Target_1_Concentration': ' ',
                'Target_1_Units': ' ',
                'Target_1_Detection Limit': ' ',
                'Target_2': ' ',
                'Target_2_Type': ' ',
                'Target_2_Result': ' ',
                'Target_2_Concentration': ' ',
                'Target_2_Units': ' ',
                'Target_2_Detection Limit': ' ',
                'Passed_QA_QC': ' ',
                'flow': ' ',
                'Flow_Units': ' ',
                'ph': ' ',
                'tss': ' ',
                'conductivity': ' ',
                'Water_Temperature': ' ',
                'notes': ' '
            });

            //parses the Sample data from the old csv object to a new object
            //this circumvents the issue of the site object outputting a csv file with the first row empty
            for(var n = 1; n < samples.length; n++){
                newSamples[n-1] = samples[n];
            }

            //outputs the parsed Samples data as an csv file
            csvWriterSamples.writeRecords(newSamples);
        }
    });