var request = require('request'),
    path = require('path');

var csv_url = function(user, form) {
    return 'http://formhub.org/' + user + '/forms/' + form +
          '/data.csv';
};

var fh_download = function(formname) {
    //check auth
    var auth = require(path.resolve(process.cwd(), './auth'));
    if (!auth) {
        console.log("please make sure auth.json in your directory");
        process.exit();
    }
    var rec = request.get({url: csv_url(auth.user, formname), auth: auth});
    rec.on('error', function(err) {
        console.log(err);
    });
    return rec;
};


module.exports = fh_download;
