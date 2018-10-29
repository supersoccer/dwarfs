const { Config, Yggdrasil, Utils } = require('@supersoccer/misty-loader')
const _ = Utils.Lodash
const mysql = require('mysql')

class Dwarfs {
  constructor () {
    this.pool = {}
    this.pool['misty'] = mysql.createPool(Config.Dwarfs.misty)
    this.cache = {}
    this.cache['misty'] = new Yggdrasil('misty')
  }

  async get (opt) {
    // let app = opt.app || 'misty'
    let app = 'misty'
    let key = opt.key
    let query = opt.query

    if (_.isUndefined(key)) {}

    return await new Promise((resolve, reject) => {
      if (typeof this.cache[app] === 'undefined') {
        throw new Error(`[dwarfs] cache driver not found: ${app}`)
      }
      this.cache[app].get(key).then(data => {
        if (!_.isEmpty(data)) {
          resolve(data)
        } else {
          this.pool[app].query(query, (error, results, fields) => {
            if (error) {
              reject(error)
            } else {
              if (!_.isEmpty(key) && !_.isUndefined(key)) {
                this.cache[app].set(key, results)
              }
              resolve(results)
            }
          })
        }
      })
        .catch(err => {
          reject(err)
        })
    })
  }

  toApiResponse (resources) {
    const data = []

    for (let resource of resources) {
      const _resource = {}
      for (let attribute of Object.keys(resource)) {
        const value = resource[attribute]
        if (attribute === 'id') {
          _resource.id = value
        } else {
          if (typeof _resource.attributes === 'undefined') {
            _resource.attributes = {}
          }
          _resource.attributes[attribute] = value
        }
      }
      data.push(_resource)
    }

    return data
  }

  getDataAndTableDescription (appName, key, query) {
    return new Promise((resolve, reject) => {
      this.get({
        app: appName,
        key: key,
        query: query
      }).then(data => {
        const queries = query.sql.split(' ')
        const queriesLowerCase = query.sql.toLowerCase().split(' ')
        const tableName = queries[queriesLowerCase.indexOf('from') + 1]

        this.get({
          app: appName,
          key: `struct:${tableName}`,
          query: {
            sql: `SHOW FULL COLUMNS FROM ${tableName}`
          }
        }).then(descriptions => {
          resolve({
            query: query,
            data: this.toApiResponse(data),
            descriptions: descriptions
          })
        }).catch(err => {
          reject(err)
        })
      }).catch(err => {
        reject(err)
      })
    })
  }

  runQuery (query) {
    let app = 'misty'
    return new Promise((resolve, reject) => {
      this.pool[app].query(query, (error, results, fields) => {
        if (error) {
          reject(error)
        } else {
          resolve(results)
        }
      })    
    })
  }
}

module.exports = new Dwarfs()
