var request = require('request'),
    fs = require('fs');
    auth = require('../auth');

var formname = "education_mopup";

var csv_url = function(user, form) {
    return 'http://formhub.org/' + user + '/forms/' + form +
          '/data.csv';
};

var rec = request.get({url: csv_url(auth.user, formname), auth: auth});

rec.on('error', function(err) {
    console.log(err);
});

rec.pipe(ws);

module.exports = {
    rec: rec
};

