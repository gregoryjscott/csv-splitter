var rec = require('./pull_fh').rec;

var csv_splitter = require('./csv_split');

var fs = require('fs');
var file = fs.createReadStream('../raw_data/education_mopup_2014_04_21_18_54_40.csv');

var split = new csv_splitter(file, '../output', 'lga');

split.run();
