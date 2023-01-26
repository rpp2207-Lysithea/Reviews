const { Client } = require('pg');
const fs = require('fs');
const { parse } = require('csv-parse');

const client = new Client({ database: 'reviews' });

client.connect();

var productsParser = parse({columns: true}, function (err, data) {
  if (err) throw err;
  var promises = [];
  while(data.length !== 0) {
    var unit = data.shift();
    console.log('PARSED product_id ' + unit.id);
    promises.push(new Promise((resolve, reject) => {
      var text = 'INSERT INTO Meta(id, name) VALUES($1, $2)';
      var values = [unit.id, unit.name];
      client.query(text, values, (err, res) => {
        if (err) reject(err);
        else{
          console.log('INSERTED product_id ' + values[0]);
          resolve(res);
        }
      });
    }));
  }
  Promise.all(promises)
    .then(() => {
      delete promises;
      fs.createReadStream(__dirname + '/data/reviews.csv').pipe(reviewsParser);
    })
    .catch((err) => {
      console.log(err);
      client.end();
    });
});

var reviewsParser = parse({columns: true}, function (err, data) {
  console.log('here');
  if (err) throw err;
  var partialParser = () => {
    var promises = [];
    console.log('here');
    for (let i = 0 ; i < 50000 && data.length !== 0; i++) {
      var unit = data.shift();
      console.log('PARSED review_id ' + unit.id);
      promises.push(new Promise((resolve, reject) => {
        var text = `INSERT INTO Reviews
        (id, product_id, rating, date, summary, body, recommend, reported, reviewer_name, reviewer_email, response, helpfulness)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`;
        var values = [
          unit.id,
          unit.product_id,
          unit.rating,
          (new Date(parseInt(unit.date))),
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
            console.log('INSERTED review_id ' + values[0]);
            resolve(res);
          }
        });
      }));
    }
    Promise.all(promises)
      .then(() => {
        delete promises;
        if (data.length !== 0) { partialParser(); }
        else { fs.createReadStream(__dirname + '/data/reviews_photos.csv').pipe(photosParser); }
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  };
  partialParser();
});

var photosParser = parse({columns: true}, function (err, data) {
  if (err) throw err;
  var promises = [];
  while(data.length !== 0) {
    var unit = data.shift();
    console.log('PARSED photo_id ' + unit.id);
    promises.push(new Promise((resolve, reject) => {
      var text = 'INSERT INTO Photos(id, review_id, url) VALUES($1, $2, $3)';
      var values = [unit.id, unit.review_id, unit.url];
      client.query(text, values, (err, res) => {
        if (err) reject(err);
        else{
          console.log('INSERTED photo_id ' + values[0]);
          resolve(res);
        }
      });
    }));
  }
  Promise.all(promises)
    .then(() => {
      delete promises;
      fs.createReadStream(__dirname + '/data/characteristics.csv').pipe(characteristicsParser);
    })
    .catch((err) => {
      console.log(err);
      client.end();
    });
});

var characteristicsParser = parse({columns: true}, function (err, data) {
  if (err) throw err;
  var promises = [];
  while(data.length !== 0) {
    var unit = data.shift();
    console.log('PARSED characteristic_id ' + unit.id);
    promises.push(new Promise((resolve, reject) => {
      var text = 'INSERT INTO Characteristics(id, product_id, name) VALUES($1, $2, $3)';
      var values = [unit.id, unit.product_id, unit.name];
      client.query(text, values, (err, res) => {
        if (err) reject(err);
        else{
          console.log('INSERTED characteristic_id ' + values[0]);
          resolve(res);
        }
      });
    }));
  }
  Promise.all(promises)
    .then(() => {
      delete promises;
      fs.createReadStream(__dirname + '/data/characteristic_reviews.csv').pipe(charRevParser);
    })
    .catch((err) => {
      console.log(err);
      client.end();
    });
});

var charRevParser = parse({columns: true}, function (err, data) {
  if (err) throw err;
  var promises = [];
  while(data.length !== 0) {
    var unit = data.shift();
    console.log('PARSED characteristics_reviews_id ' + unit.id);
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
          console.log('INSERTED characteristics_reviews_id ' + values[0]);
          resolve(res);
        }
      });
    }));
  }
  Promise.all(promises)
    .then(() => {
      delete promises;
      console.log();
      console.log('====== ETL COMPLETED ======');
      client.end();
    })
    .catch((err) => {
      console.log(err);
      client.end();
    });
});

fs.createReadStream(__dirname + '/data/reviews.csv').pipe(reviewsParser);