const { Client } = require('pg');

module.exports = new Client({
    database: 'reviews',
    user: 'ubuntu',
    password: 'root',
});