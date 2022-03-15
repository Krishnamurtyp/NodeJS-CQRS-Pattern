const {v4: uuid} = require('uuid');

function primeRequestContext(req, res, next) {
    req.context = {
        traceId: uuid(undefined, undefined, undefined)
    }

    next()
}

module.exports = primeRequestContext;