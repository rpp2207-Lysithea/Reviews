const { Client } = require('pg');
const formatter = require('./helpers.js').formatter;

const client = new Client({ database: 'reviews' });

var ETL = async (arr) => {
  client.connect();
  for (file of arr) {
    await formatter(file.path, file.table, file.columns, client);
  }
  client.end();
};

var tableData = [
  {
    path: 'reviews',
    table: 'reviews',
    columns: [
      'review_id',
      'product_id',
      'rating',
      'date',
      'summary',
      'body',
      'recommend',
      'reported',
      'reviewer_name',
      'reviewer_email',
      'response',
      'helpfulness'
    ]
  },
  // {
  //   path: 'reviews_photos',
  //   table: 'photos',
  //   columns: [
  //     'id',
  //     'review_id',
  //     'url'
  //   ]
  // },
  // {
  //   path: 'characteristics',
  //   table: 'characteristics',
  //   columns: [
  //     'id',
  //     'product_id',
  //     'name'
  //   ]
  // },
  // {
  //   path: 'characteristic_reviews',
  //   table: 'characteristic_reviews',
  //   columns: [
  //     'id',
  //     'characteristic_id',
  //     'review_id',
  //     'value'
  //   ]
  // }
];

ETL(tableData);

// console.log(new Date(1596080481467));