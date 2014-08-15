var fs = require('fs'),
    mkdirp = require('mkdirp'),
    path = require('path'),
    program = require('commander'),
    formhub_dl = require('./pull_fh'),
    splitter = require('./csv_split');

program
    .version('0.0.1')
    .description('split an input csv into smaller csvs based on one column')
    .option('-i, --input <file>', 'select an input csv')
    .option('-f, --formhub <dataset>', 'select a formhub dataset as input')
    .option('-o, --output <directory>', 'select an output directory')
    .option('-b, --groupby <group>', 'the column you want to group by')
    .parse(process.argv);            

var action = function() {
        var input_file = program.input;
        var fh_dataset = program.formhub;
        if (!(input_file || fh_dataset)) {
            console.log("please specify a file or formhub input");
            process.exit();
        }
        if (input_file) {
            var input_stream = fs.createReadStream(input_file);
        }
        if (fh_dataset) {
            var input_stream = formhub_dl(fh_dataset);
        }
        return ensure_dir(program.output, function() {
            var split = new splitter(input_stream,
                                     program.output,
                                     program.groupby);
            split.run();
        });
};

var ensure_dir = function(file_path, cb) {
    var dirname = path.resolve(file_path);
    fs.exists(dirname, function(exist) {
        if (exist) { cb(); }
        else {
            mkdirp(dirname, function(err) {
                if(err) { 
                    console.log(err); 
                    process.exit(1);
                }
                cb();
            });
        }
    });
};

action();
