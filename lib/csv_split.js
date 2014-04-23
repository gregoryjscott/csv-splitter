var fs = require("fs"),
    through = require("through"),
    csv = require("csv-streamify"),
    path = require("path"),
    out_dir = "../output",
    write_streams = {},
    header = '',
    group_sequence;

var rec = require('./pull_fh').rec;
    

var abs_resolve = function(relative_location) {
    return path.resolve(__dirname, relative_location);
};

var write_csv_line = function(arr) {
    //adding quotes
    var quoted_arr = arr.map(function(item) {
        return '\"' + item + '\"';
    });
    return quoted_arr.toString() + '\n';
};


var split = through(function(data) {
    var name = data[group_sequence];
    var write_obj = {"name" : name,
                     "data" : data
                    };
    this.queue(write_obj);
});

var drain = function(data) {
    var name = data.name;
    var outfile = path.resolve(out_dir, name + ".csv");
    if (write_streams[name]) {
        write_streams[name].write(write_csv_line(data.data));
    } else {
        var ws = fs.createWriteStream(outfile);
        write_streams[name] = ws;
        ws.write(write_csv_line(header));
        ws.write(write_csv_line(data.data));
    }
};

var parse = function(group_name) {
    var parser = csv({objectMode: true});
    parser.on('readable', function() {
        if (parser.lineNo === 0) {
            header = parser.read();
            group_sequence = header.indexOf(group_name);
        }
    });
    return parser;
};



var read_file = process.argv.pop();
var read_stream = fs.createReadStream(abs_resolve(read_file));
//read_stream
rec
    .pipe(parse('lga'))
    .pipe(split)
    .on('data', function(data){
        drain(data);
    });
