var fs = require('fs'),
    mkdirp = require('mkdirp'),
    path = require('path'),
    program = require('commander'),
    splitter = require('./csv_split');

program
    .version('0.0.1')
    .description('split an input csv into smaller csvs based on one column')
    .option('-i, --input <file>', 'select an input csv')
    .option('-o, --output <directory>', 'select an output directory')
    .option('-b, --groupby <group>', 'the column you want to group by')
    .parse(process.argv);            

var action = function() {
        var input_file = program.input;
        var input_stream;
        if(!program.groupby) {
            console.log("please specify which column you would like to split" +
                   " your file by");
            process.exit();
        }
        if (!(input_file)) {
            input_stream = process.stdin;
        } else {
            input_stream = fs.createReadStream(input_file);
        } 
        if (!program.output) {
            var file_name = program.input ? 
                path.basename(program.input).replace(/\.csv$/,'') :
                'csvfile';
            program.output = [file_name, 'by', program.groupby].join('_');
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
