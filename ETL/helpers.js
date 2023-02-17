const fs = require('fs');
const format = require('pg-format');
const { parse } = require('csv-parse');

exports.getData = (fileName) => {
    return new Promise((resolve, reject) => {
        fs.createReadStream(__dirname + `/data/${fileName}.csv`)
            .pipe(parse({
                cast: (value, context) => {
                    if (context.column === 0) console.log('CASTING review_id ' + value);
                    if (fileName === 'reviews' && context.column === 3) return new Date(parseInt(value));
                    return value;
                }
            }, (err, data) => {
                if (err)
                    reject(err);
                else
                    resolve(data);
            }));
    });
};
exports.runner = async (queries) => {
    await Promise.all(queries)
        .catch((err) => {
            console.log(err);
        });
};
exports.formatter = async (path, table, cols, client) => {
    var data = await this.getData(path)
        .catch((err) => {
            console.log(err);
        });
    data.shift();
    var queries = [];
    var text = `INSERT INTO ${table} (${cols.toString()}) VALUES %L`;
    var count = 1;
    while (data.length !== 0) {
        var query = format(text, data.slice(0, 1000));
        queries.push(new Promise((resolve, reject) => {
            var cnt = count;
            client.query(query, (err, res) => {
                if (err)
                    reject(err);
                else {
                    console.log(`INSERTED ${path} BATCH #${cnt}`);
                    resolve(res);
                }
            });
        }));
        data = data.slice(1000);
        console.log(`FORMATTED ${path} BATCH #${count}`);
        count++;
    }
    await this.runner(queries);
    queries = null;
    console.log(`COMPLETED ${path} TRANSFER INTO ${table}`);
};