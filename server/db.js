const { Client } = require('pg');

module.exports = new Client({
    database: 'reviews',
    user: 'jackbossert',
    password: ''
});