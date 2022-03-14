const uuid = require('uuid/v4')

const seedVideos = [
    {
        owner_id: uuid(),
        name: `video ${uuid()}`,
        description: 'Best video ever',
        transcoding_status: 'transcoded',
        view_count: 0
    },
    {
        owner_id: uuid(),
        name: `video ${uuid()}`,
        description: 'Even more best video',
        transcoding_status: 'transcoded',
        view_count: 1
    },
    {
        owner_id: uuid(),
        name: `video ${uuid()}`,
        description: 'Even still more best video',
        transcoding_status: 'transcoded',
        view_count: 2
    }
]

exports.seed = knex =>
    knex('videos')
        .del()
        .then(() => knex('videos').insert(seedVideos))
