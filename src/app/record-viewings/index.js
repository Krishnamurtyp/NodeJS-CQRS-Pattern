const express = require('express');

function createAction({db}){
    function recordViewing(traceId, videoId){
        return Promise.resolve(true)
    }
    return{
        recordViewing
    }
}

function createHandlers({action}){
    function handleRecordViewing(req, res){
        return actions.recordViewing(req.context.traceId, req.params.videoId).then(()=>res.redirect('/'))
    }
    return{
        handleRecordViewing
    }
}

function createRecordViewings({}){
    const action = createAction({})

    const handlers = createHandlers({action})

    const router = express.Router()

    router.route('/:videoId').post(handlers.handleRecordViewing)

    return{action, handlers, router}
}

module.exports = createRecordViewings