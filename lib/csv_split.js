var fs = require("fs"),
    through = require("through2"),
    csv = require("csv-parser"),
    _ = require("lodash"),
    path = require("path");

var Splitter = function(input_stream, output_dir, group_by) {
    // initiate the prototype class
    this.input_stream = input_stream;
    this.output_dir = output_dir;
    this.group_by = group_by;
    this.write_streams = {};
};

Splitter.prototype.split = function() {
    // find the value of the target column, then
    // cast it in an object, pass downstream
    var self = this;
    return through.obj(function(chunk, enc, cb) {
        var name = clean_name(chunk[self.group_by]);
        var write_obj = {"name" : name,
                         "data" : chunk
                        };
        this.push(write_obj);
        cb();
    });
};

Splitter.prototype.drain = function(data) {
    // writing function
    var self = this;
    var name = data.name;
    var outfile = path.resolve(this.output_dir, name + ".csv");
    if (this.write_streams[name]) {
        this.write_streams[name].write(write_csv_line(_.values(data.data)));
    } else {
        var ws = fs.createWriteStream(outfile);
        this.write_streams[name] = ws;
        ws.write(write_csv_line(_.keys(data.data)));
        ws.write(write_csv_line(_.values(data.data)));
    }
};

Splitter.prototype.run = function() {
    // pipeline of the splitter
    var self = this;
    return this.input_stream
        .pipe(csv())
        .pipe(this.split())
        .on('data', function(data){
            self.drain(data);
        });
};

var write_csv_line = function(arr) {
    //adding quotes
    var quoted_array = arr.map(function(item) {
        return '\"' + item + '\"';
    });
    return quoted_array.toString() + '\n';
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
