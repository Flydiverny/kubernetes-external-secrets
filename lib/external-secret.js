'use strict'

/**
 * Block asynchronous flow.
 * @param {number} milliseconds - Number of milliseconds to block flow operation.
 * @returns {Promise} Promise object representing block flow operation.
 */
function sleep ({ milliseconds }) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

const difference = (a, b) => {
  const s = new Set(b)
  return a.filter(x => !s.has(x))
}

/**
 * Get a stream of external secret events. This implementation uses
 * polling/batching and converts it to a stream of events.
 * @param {Object} kubeClient - Client for interacting with kubernetes cluster.
 * @param {Object} customResourceManifest - Custom resource manifest.
 * @param {number} intervalMilliseconds - Interval time in milliseconds for polling external secrets.
 * @param {Object} logger - Logger for logging stuff.
 * @returns {Object} An async generator that yields externalsecret events.
 */
function getExternalSecretEvents ({
  kubeClient,
  customResourceManifest,
  intervalMilliseconds,
  logger
}) {
  return (async function * () {
    const trackedSecrets = new Map()

    while (true) {
      try {
        const externalSecrets = await kubeClient
          .apis[customResourceManifest.spec.group]
          .v1[customResourceManifest.spec.names.plural]
          .get()

        const deletedExternalSecrets = difference(Array.from(trackedSecrets.keys()), externalSecrets.body.items.map(es => es.metadata.uid))

        for (const externalSecretId of deletedExternalSecrets) {
          const deletedExternalSecret = trackedSecrets.get(externalSecretId)
          trackedSecrets.delete(externalSecretId)
          logger.info('deleted %s', deletedExternalSecret.metadata.selfLink)

          yield {
            type: 'DELETED',
            object: deletedExternalSecret
          }
        }

        for (const externalSecret of externalSecrets.body.items) {
          const existingExternalSecret = trackedSecrets.get(externalSecret.metadata.uid)
          if (existingExternalSecret) {
            if (existingExternalSecret.metadata.resourceVersion !== externalSecret.metadata.resourceVersion) {
              logger.info('modified %s', externalSecret.metadata.selfLink)
              trackedSecrets.set(externalSecret.metadata.uid, externalSecret)

              yield {
                type: 'MODIFIED',
                object: externalSecret
              }
            } else {
              logger.debug('no change detected for %s', externalSecret.metadata.selfLink)
            }
          } else {
            logger.info('added %s', externalSecret.metadata.selfLink)
            trackedSecrets.set(externalSecret.metadata.uid, externalSecret)

            yield {
              type: 'ADDED',
              object: externalSecret
            }
          }
        }
      } catch (err) {
        logger.warn(err, 'Failed to fetch external secrets')
      }
      await sleep({ milliseconds: intervalMilliseconds })
    }
  }())
}

module.exports = {
  getExternalSecretEvents
}
