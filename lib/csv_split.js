var fs = require("fs"),
    through = require("through"),
    csv = require("csv-streamify"),
    path = require("path");

var Splitter = function(input_stream, output_dir, group_by) {
    this.input_stream = input_stream;
    this.output_dir = output_dir;
    this.group_by = group_by;
    this.write_streams = {};
    this.header = null;
    this.group_sequence = null;
};

Splitter.prototype.write_csv_line = function(arr) {
    //adding quotes
    var quoted_arr = arr.map(function(item) {
        return '\"' + item + '\"';
    });
    return quoted_arr.toString() + '\n';
};


Splitter.prototype.split = function() {
    var self = this;
    return through(function(data) {
        var name = clean_name(data[self.group_sequence]);
        var write_obj = {"name" : name,
                         "data" : data
                        };
        this.queue(write_obj);
    });
};

Splitter.prototype.drain = function(data) {
    var self = this;
    var name = data.name;
    var outfile = path.resolve(this.output_dir, name + ".csv");
    if (this.write_streams[name]) {
        this.write_streams[name].write(this.write_csv_line(data.data));
    } else {
        var ws = fs.createWriteStream(outfile);
        this.write_streams[name] = ws;
        ws.write(this.write_csv_line(this.header));
        ws.write(this.write_csv_line(data.data));
    }
};

Splitter.prototype.parse = function(group_name) {
    var self = this;
    var parser = csv({objectMode: true});
    parser.on('readable', function() {
        if (parser.lineNo === 0) {
            self.header = parser.read();
            self.group_sequence = self.header.indexOf(group_name);
        }
    });
    return parser;
};

Splitter.prototype.run = function() {
    var self = this;
    return this.input_stream
        .pipe(this.parse(this.group_by))
        .pipe(this.split())
        .on('data', function(data){
            self.drain(data);
        });
};

var clean_name = function(file_name) {
    // file_name cannot have '/', '\' in it
    // convert them into underscore
    var forbidden_char = /(\/|\\)/g;
    var replacement = '_';
    if (file_name.match(forbidden_char)){
        return file_name.replace(forbidden_char, replacement);
    }
    return file_name;
};

module.exports = Splitter;
