function createHandlers ({ queries }) {
    return {
        VideoViewed: event => queries.incrementVideosWatched(event.globalPosition)
    }
}

function createQueries ({ db }) {
    function ensureHomePage () {
        const initialData = {
            pageData: { lastViewProcessed: 0, videosWatched: 0 }
        }

        const queryString = `
      INSERT INTO
        pages(page_name, page_data)
      VALUES
        ('home', :pageData)
      ON CONFLICT DO NOTHING
    `
        return db.then(client => client.raw(queryString, initialData))
    }

    function incrementVideosWatched (globalPosition) {
        const queryString = `
      UPDATE
        pages
      SET
        page_data = jsonb_set(
          jsonb_set(
            page_data,
            '{videosWatched}',
            ((page_data ->> 'videosWatched')::int + 1)::text::jsonb
          ),
          '{lastViewProcessed}',
          :globalPosition::text::jsonb
        )
      WHERE
        page_name = 'home' AND
        (page_data->>'lastViewProcessed')::int < :globalPosition
    `

        return db.then(client => client.raw(queryString, { globalPosition }))
    }

    return {
        ensureHomePage,
        incrementVideosWatched
    }
}

function build ({ db, messageStore }) {
    const queries = createQueries({ db })
    const handlers = createHandlers({ queries })
    const subscription = messageStore.createSubscription({
        streamName: 'viewing',
        handlers,
        subscriberId: 'aggregators:home-page'
    })

    function init () {
        return queries.ensureHomePage()
    }

    function start () {
        init().then(subscription.start)
    }

    return {
        queries,
        handlers,
        init,
        start
    }
}

module.exports = build