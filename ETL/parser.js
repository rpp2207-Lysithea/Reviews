const { parse } = require('csv-parse');

module.exports = {
  products: parse({columns: true}, function (err, data) {
    var promises = [];
    while(data.length !== 0) {
      var unit = data.shift();
      console.log('PARSED : ' + unit.id);
      promises.push(new Promise((resolve, reject) => {
        var text = 'INSERT INTO Meta(id, name) VALUES($1, $2)';
        var values = [unit.id, unit.name];
        client.query(text, values, (err, res) => {
          if (err) reject(err);
          else{
            console.log('QUERIED : ' + unit.id);
            resolve(res);
          }
        });
      }));
    }
    Promise.all(promises)
      .then((values) => {
        console.log(values.length + ' INSERTED');
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  }),

  reviews: parse({columns: true}, function (err, data) {
    var promises = [];
    while(data.length !== 0) {
      var unit = data.shift();
      console.log('PARSED : ' + unit.id);
      promises.push(new Promise((resolve, reject) => {
        var text = `INSERT INTO Reviews
        (id, product_id, rating, date, summary, body, recommend, reported, reviewer_name, reviewer_email, response, helpfulness)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`;
        var values = [
          unit.id,
          unit.product_id,
          unit.rating,
          unit.date,
          unit.summary,
          unit.body,
          unit.recommend,
          unit.reported,
          unit.reviewer_name,
          unit.reviewer_email,
          unit.response,
          unit.helpfulness
        ];
        client.query(text, values, (err, res) => {
          if (err) reject(err);
          else{
            console.log('QUERIED : ' + unit.id);
            resolve(res);
          }
        });
      }));
    }
    Promise.all(promises)
      .then((values) => {
        console.log(values.length + ' INSERTED');
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  }),

  photos: parse({columns: true}, function (err, data) {
    var promises = [];
    while(data.length !== 0) {
      var unit = data.shift();
      console.log('PARSED : ' + unit.id);
      promises.push(new Promise((resolve, reject) => {
        var text = 'INSERT INTO Photos(id, review_id, url) VALUES($1, $2, $3)';
        var values = [unit.id, unit.review_id, unit.url];
        client.query(text, values, (err, res) => {
          if (err) reject(err);
          else{
            console.log('QUERIED : ' + unit.id);
            resolve(res);
          }
        });
      }));
    }
    Promise.all(promises)
      .then((values) => {
        console.log(values.length + ' INSERTED');
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  }),

  characteristics: parse({columns: true}, function (err, data) {
    var promises = [];
    while(data.length !== 0) {
      var unit = data.shift();
      console.log('PARSED : ' + unit.id);
      promises.push(new Promise((resolve, reject) => {
        var text = 'INSERT INTO Characteristics(id, product_id, name) VALUES($1, $2, $3)';
        var values = [unit.id, unit.product_id, unit.name];
        client.query(text, values, (err, res) => {
          if (err) reject(err);
          else{
            console.log('QUERIED : ' + unit.id);
            resolve(res);
          }
        });
      }));
    }
    Promise.all(promises)
      .then((values) => {
        console.log(values.length + ' INSERTED');
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  }),

  char_rev: parse({columns: true}, function (err, data) {
    var promises = [];
    while(data.length !== 0) {
      var unit = data.shift();
      console.log('PARSED : ' + unit.id);
      promises.push(new Promise((resolve, reject) => {
        var text = `INSERT INTO
        Characteristics_Reviews(id, characteristic_id, review_id, value)
        VALUES($1, $2, $3, $4)`;
        var values = [
          unit.id,
          unit.characteristic_id,
          unit.review_id,
          unit.value
        ];
        client.query(text, values, (err, res) => {
          if (err) reject(err);
          else{
            console.log('QUERIED : ' + unit.id);
            resolve(res);
          }
        });
      }));
    }
    Promise.all(promises)
      .then((values) => {
        console.log(values.length + ' INSERTED');
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  })
}