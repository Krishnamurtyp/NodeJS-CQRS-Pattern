const express = require('express');
const { v4: uuid } = require('uuid');

function createAction({messageStore}){
    function recordViewing(traceId, videoId, userId){
        const viewedEvent = {
            id:uuid(undefined, undefined, undefined),
            type: 'VideoViewed',
            metadata: {
                traceId,
                userId
            },
            data:{
                userId,
                videoId
            }
        }

        const streamName = `viewing-${videoId}`;

        return messageStore.write(streamName, viewedEvent)
    }
    return{
        recordViewing
    }
}

function createHandlers({actions}){
    function handleRecordViewing(req, res){
        return actions.recordViewing(req.context.traceId, req.params.videoId, req.context.userId).then(()=>res.redirect('/'))
    }
    return{
        handleRecordViewing
    }
}

function createRecordViewings({messageStore}){
    const action = createAction({messageStore})

    const handlers = createHandlers({action})

    const router = express.Router()

    router.route('/:videoId').post(handlers.handleRecordViewing)

    return{action, handlers, router}
}

module.exports = createRecordViewings