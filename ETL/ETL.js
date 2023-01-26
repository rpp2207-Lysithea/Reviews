const { Client } = require('pg');
const fs = require('fs');
const { parse } = require('csv-parse');
const { format } = require('date-fns');

const client = new Client({ database: 'reviews' });

client.connect();

const parser = {
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
          (new Date(unit.date)).toString(),
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
        client.end();
      })
      .catch((err) => {
        console.log(err);
        client.end();
      });
  })
};

async function ETL() {
  // const p = await fs.createReadStream(__dirname + '/testData/products.csv').pipe(parser.products);
  // const r = await fs.createReadStream(__dirname + '/testData/reviews.csv').pipe(parser.reviews);
  // const rp = await fs.createReadStream(__dirname + '/testData/reviews_photos.csv').pipe(parser.photos);
  // const c = await fs.createReadStream(__dirname + '/testData/characteristics.csv').pipe(parser.characteristics);
  const cr = await fs.createReadStream(__dirname + '/testData/characteristics_reviews.csv').pipe(parser.char_rev);


  // fs.createReadStream(__dirname + '/data/products.csv').pipe(parser.products);
  // fs.createReadStream(__dirname + '/data/reviews.csv').pipe(parser.reviews);
  // fs.createReadStream(__dirname + '/data/reviews_photos.csv').pipe(parser.photos);
  // fs.createReadStream(__dirname + '/data/characteristics.csv').pipe(parser.characteristics);
  // fs.createReadStream(__dirname + '/data/characteristics_reviews.csv').pipe(parser.characteristics);
}

ETL();
console.log('DONE');
// client.end();
// var metaParser = parse({columns: true}, function (err, data) {
//   var promises = [];
//   while(data.length !== 0) {
//     var unit = data.shift();
//     console.log('PARSED : ' + unit.id);
//     promises.push(new Promise((resolve, reject) => {
//       var text = 'INSERT INTO Meta(id, name) VALUES($1, $2)';
//       var values = [unit.id, unit.name];
//       client.query(text, values, (err, res) => {
//         if (err) reject(err);
//         else{
//           console.log('QUERIED : ' + unit.id);
//           resolve(res);
//         }
//       });
//     }));
//   }
//   Promise.all(promises)
//     .then((values) => {
//       console.log(values.length + ' INSERTED');
//       client.end();
//     })
//     .catch((err) => {
//       console.log(err);
//       client.end();
//     });
// });

// var reviewParser = parse({columns: true}, function (err, data) {
//   var promises = [];
//   while(data.length !== 0) {
//     var unit = data.shift();
//     console.log('PARSED : ' + unit.id);
//     promises.push(new Promise((resolve, reject) => {
//       var text = `INSERT INTO Reviews
//       (id, product_id, rating, date, summary, body, recommend, reported, reviewer_name, reviewer_email, response, helpfulness)
//       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`;
//       var values = [
//         unit.id,
//         unit.product_id,
//         unit.rating,
//         unit.date,
//         unit.summary,
//         unit.body,
//         unit.recommend,
//         unit.reported,
//         unit.reviewer_name,
//         unit.reviewer_email,
//         unit.response,
//         unit.helpfulness
//       ];
//       client.query(text, values, (err, res) => {
//         if (err) reject(err);
//         else{
//           console.log('QUERIED : ' + unit.id);
//           resolve(res);
//         }
//       });
//     }));
//   }
//   Promise.all(promises)
//     .then((values) => {
//       console.log(values.length + ' INSERTED');
//       client.end();
//     })
//     .catch((err) => {
//       console.log(err);
//       client.end();
//     });
// });