const express = require("express");
const { rmSync } = require("fs");
const { Client } = require('pg');
const format = require('pg-format');

const client = new Client({ 
  database: 'reviews',
  password: ''
});

const app = express();

app.get('/', (req, res) => {
  res.status(200);
  res.send('WELCOME!');
});

app.get('/reviews/', (req, res) => {
  let page = !parseInt(req.query.page) ? 1 : parseInt(req.query.page);
  let count = !parseInt(req.query.count) ? 5 : parseInt(req.query.count);
  let sort = req.query.sort === 'newest' ? 'date' : req.query.sort === 'helpful' ? 'helpfulness' : 'review_id'; // ID is temporary until I discover what relevance is
  let pid = req.query.product_id;
  var query = `
  SELECT 
    review_id,
    rating,
    summary,
    recommend,
    response,
    body,
    date,
    reviewer_name,
    helpfulness,
    json_strip_nulls(json_agg(json_build_object('id',id::integer,'url',url::text)))
      AS photos 
  FROM (SELECT r.*, p.id, p.url
    FROM Reviews r
    LEFT OUTER JOIN Photos p 
    ON p.review_id = r.review_id) AS tmp
  WHERE product_id = ${pid}
  GROUP BY tmp.review_id, tmp.rating, tmp.summary, tmp.recommend, tmp.response, tmp.body, tmp.date, tmp.reviewer_name, tmp.helpfulness
  ORDER BY tmp.${sort}
  LIMIT ${count}
  OFFSET ${(page - 1) * count}`;
  client.query(query, (err, result) => {
    if (err) {
      res.status(404);
      res.send(err);
    }
    else {
      res.status(200);
      res.send({
        "product": pid,
        "page": page,
        "count": count,
        "results": result.rows
      });
    }
  });
});

app.get('/reviews/meta', (req, res) => {
  let pid = req.query.product_id;

  let query = `
  SELECT 
    json_object_agg(rt.rating::text, rt.count::int) 
      AS ratings,
    json_object_agg(rm.recommend::text, rm.count::int) 
      AS recommended,
    json_object_agg(c.name::text, json_build_object('id',c.id::int,'value',c.value::decimal)) 
      AS characteristics  
  FROM Ratings rt
    INNER JOIN Recommended rm 
      ON rm.product_id = rt.product_id
    INNER JOIN Characteristics c 
      ON c.product_id = rt.product_id 
        AND rt.product_id = ${pid}`;

  client.query(query, (err, result) => {
    if (err) {
      res.status(404);
      res.send(err);
    } else {
      res.status(200);
      res.send({
        "product_id": pid,
        "ratings": result.rows[0]["ratings"],
        "recommended": result.rows[0]["recommended"],
        "characteristics": result.rows[0]["characteristics"]
      });
    };
  });
});

// IMPLEMENT FUNCTION BEFORE OR AFTER POST INSTEAD OF TRIGGERS
  // Make array of queries, forEach run query;
  // STILL NEED TO MAKE CHARACTERISTIC VALUE QUERY
app.post('/reviews', (req, res) => {
  let { pid, rate, sum, body, rec, name, email, photos, char } = req.query;
  let rid;

  let revQuery = `
  INSERT INTO Reviews 
    (product_id, rating, summary, body, recommend, name, email) 
  VALUES 
    (${pid}, ${rate}, ${sum}, ${body}, ${rec}, ${name}, ${email})
  RETURNING review_id`;
  
  client.query(revQuery, (err, result) => {
    if (err) res.sendStatus(409);
    else {
      rid = result.rows[0]["id"];
      photos = photos.map((photo) => {
        return [rid, photo];
      });
      char = Object.keys(char).map((key) => {
        return [key, rid, char[key]];
      });
      var queries = [
        format(
          `INSERT INTO Photos 
            (review_id, url) 
          VALUES %L`,photos
        ),

        format(
          `INSERT INTO Characteristic_reviews 
            (characteristic_id, review_id, value) 
          VALUES %L`, char
        ),
        // `IF EXISTS (SELECT * FROM Ratings WHERE product_id = ${pid} AND rating = ${rate})
        //   UPDATE Ratings 
        //   SET count = count + 1 WHERE product_id = ${pid} AND rating = ${rate}
        // ELSE
        //   INSERT INTO Recommended (product_id, recommend) VALUES (${pid}, ${rec})`,
  
        // `IF EXISTS (SELECT * FROM Recommended WHERE product_id = ${pid} AND recommend = ${rec})
        //   UPDATE Recommended 
        //   SET count = count + 1 WHERE product_id = ${pid} AND recommend = ${rec}
        // ELSE
        //   INSERT INTO Recommended (product_id, recommend) VALUES (${pid}, ${rec})`,
        // `UPDATE Characteristics`
      ];
      client.query(queries[0], (err, result) => {
        if (err) res.sendStatus(409);
        else {
          client.query(queries[1], (err, result) => {
            if (err) res.sendStatus(409);
            else {
              res.sendStatus(201);
            }
          });
        }
      });
    }
  });
});

app.put('/reviews/:review_id/helpful', (req, res) => {
  let rid = req.params.review_id;
  let query = `
  UPDATE Reviews 
  SET helpfulness = helpfulness + 1
  WHERE review_id = ${rid}`;
  client.query(query, (err) => {
    if (err) res.sendStatus(409);
    else res.sendStatus(204);
  });
});

app.put('/reviews/:review_id/report', (req, res) => {
  let rid = req.params.review_id;
  let query = `
  UPDATE Reviews 
  SET reported = t 
  WHERE review_id = ${rid}`;
  client.query(query, (err) => {
    if (err) res.sendStatus(409);
    else res.sendStatus(204);
  });
});


const PORT = 3000;

app.listen(PORT, () => {
  client.connect();
  console.log(`Server listening at http://localhost:${PORT}`);
});

process.on("SIGQUIT", () => {
  console.log(`Closing server at http://localhost:${PORT}`);
  client.end();
  process.exit(0);
});