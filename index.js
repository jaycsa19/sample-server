/**
 * Hackathon example server
 * Allows getting logs from CrateDB or RethinkDB using:
 * HTTP GET /logs/cratedb?min=etc&max=etc
 * or HTTP GET /logs/rethinkdb?min=etc&max=etc
 *
 * Feel free to modify this code however you want, or delete
 * it and start over from scratch.
 */

require('dotenv/config');
const nconf = require('nconf');
const Koa = require('koa');
const Router = require('koa-router');
const crate = require('node-crate');
const logger = require('koa-logger');
const rethinkdbdash = require('rethinkdbdash');
const moment = require('moment');

const max_limit = 50000;

// Initialize configuration variables
nconf
    .argv({ parseValues: true })
    .env({ parseValues: true, lowerCase: true })
    .defaults({
        rethink_database: 'hackathon',
        rethink_port: 28015,
        crate_port: 4200,
        app_port: 8080,
	rethink_host: 'databases-internal.hackathon.venom360.com',
        crate_host: 'databases-internal.hackathon.venom360.com'
    })
    .required([
        'rethink_database',
        'rethink_host',
        'rethink_port',
        'crate_host',
        'crate_port',
        'app_port'
    ]);

// Connect to databases
const r = rethinkdbdash({
    db: nconf.get('rethink_database'),
    servers: [
        { host: nconf.get('rethink_host'), port: nconf.get('rethink_port') }
    ],
    ssl: { rejectUnauthorized: false }
});

crate.connect(nconf.get('crate_host'), nconf.get('crate_port'));

// Start web server using Koa
const app = new Koa();
const router = new Router();

app.use(logger());

// HTTP GET /logs/rethinkdb/loglevel?min=etc&max=etc&logtype=etc to get logs of a certain type between dates
router.get('/logs/rethinkdb/loglevel', async ctx => {

    const {min, max, logtype} = ctx.query;
    let entries;

    const logLevels = {
        'fatal': 60,
        'error': 50,
        'warn': 40,
        'info': 30,
        'debug': 20,
        'trace': 10
    };

    if (!logtype || logLevels[logtype] === undefined) {
        ctx.throw(400, 'Must specify logtype as (fatal, error, warn, info, debug, trace)');
    }

    if (min && max) {
        const minDate = moment.utc(min, moment.ISO_8601);
        const maxDate = moment.utc(max, moment.ISO_8601);

        if (!minDate.isValid() || !maxDate.isValid())
            ctx.throw(400, 'Min and max must be ISO 8601 date strings');
        else {
            entries = await r
                .table('logs')
                .between(minDate.toDate(), maxDate.toDate(), {index: 'time'})
		.limit(max_limit)
                .filter({level: logLevels[logtype]})
                .run();
        }
    } else {
        entries = await r
            .table('logs')
	    .limit(max_limit)
            .filter({level: logLevels[logtype]})
            .run();
    }

    ctx.status = 200;
    ctx.body = entries;
});

// HTTP GET /logs/rethinkdb?min=etc&max=etc to get logs between dates
router.get('/logs/rethinkdb', async ctx => {
    const { min, max } = ctx.query;
    if (!min || !max)
        ctx.throw(400, 'Must specify min and max in query string.');

    const minDate = moment.utc(min, moment.ISO_8601);
    const maxDate = moment.utc(max, moment.ISO_8601);

    if (!minDate.isValid() || !maxDate.isValid())
        ctx.throw(400, 'Min and max must be ISO 8601 date strings');

    const entries = await r
        .table('logs')
        .between(minDate.toDate(), maxDate.toDate(), { index: 'time' })
        .limit(max_limit)
        .run();

    ctx.status = 200;
    ctx.body = entries;
});

// HTTP GET /logs/cratedb?min=etc&max=etc to get logs between dates
router.get('/logs/cratedb', async ctx => {
    const { min, max } = ctx.query;
    if (!min || !max)
        ctx.throw(400, 'Must specify min and max in query string.');

    const minDate = moment.utc(min, moment.ISO_8601);
    const maxDate = moment.utc(max, moment.ISO_8601);

    if (!minDate.isValid() || !maxDate.isValid())
        ctx.throw(400, 'Min and max must be ISO 8601 date strings');

    const entries = await crate.execute(
        'SELECT * FROM logs WHERE time BETWEEN ? AND ? LIMIT ?',
        [minDate.toDate(), maxDate.toDate(), max_limit]
    );

    ctx.status = 200;
    ctx.body = entries.json;
});

// Use router middleware
app.use(router.routes());
app.use(router.allowedMethods());

// Start server on app port.
const port = nconf.get('app_port');
app.listen(port, () => {
    console.log(`Server started on port ${port}.`);
});
