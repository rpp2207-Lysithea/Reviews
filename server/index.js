const express = require("express");
const app = express();

app.get('/', (req, res) => {
  res.status(200);
  res.send(true);
})


const PATH = 3000;
app.listen(PATH, () => {
  console.log(`Listening on port: ${PATH}`);
});