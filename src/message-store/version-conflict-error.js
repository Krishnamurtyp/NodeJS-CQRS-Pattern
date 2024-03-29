﻿function VersionConflictError (stream, expected, actual) {
    Error.captureStackTrace(this, this.constructor)
    this.message = [
        'VersionConflict: stream',
        stream,
        'expected version',
        expected,
        'but was at version',
        actual
    ].join(' ')
    this.name = 'VersionConflictError'
}

VersionConflictError.prototype = Object.create(Error.prototype)
VersionConflictError.prototype.constructor = VersionConflictError

module.exports = VersionConflictError