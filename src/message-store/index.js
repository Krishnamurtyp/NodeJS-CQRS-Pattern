const createWrite = require('./write')
function createMessageStore({db}) {
    const write = createWrite({db})
    return {
        write:write
    }
}