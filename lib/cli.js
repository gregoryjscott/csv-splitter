var fs = require('fs'),
    program = require('commander'),
    formhub_dl = require('./pull_fh'),
    splitter = require('./csv_split');
console.log('starting', new Date());

program
    .version('0.0.1')
    .option('-i, --input <file>', 'select an input csv')
    .option('-f, --formhub <dataset>', 'select a formhub dataset as input')
    .option('-o, --output <directory>', 'select an output directory')
    .option('-b, --groupby <group>', 'the column you want to group by');

program
    .command('split')
    .description('split an input csv into smaller csvs based on one column')
    .action(function() {
        var input_file = program.input;
        var fh_dataset = program.formhub;
        if (!(input_file || fh_dataset)) {
            console.log("please specify a file or formhub input");
            process.exit();
        }
        if (input_file) {
            var split = new splitter(fs.createReadStream(input_file),
                                     program.output,
                                     program.groupby);
            return split.run();
        }
        if (fh_dataset) {
            var fh_split = new splitter(formhub_dl(fh_dataset),
                                     program.output,
                                     program.groupby);
            return fh_split.run(); 
        }
    });

program.parse(process.argv);            
process.on('exit', function() {
    console.log('finished', new Date());
});
