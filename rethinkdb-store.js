'use strict'

var _ = require('lodash')
// var r = require('rethinkdb')
var Promise = require('bluebird')
var StandardQueryPlugin = require('seneca-standard-query')
var r
var storename = 'rethinkdb-store'

module.exports = function (opts) {
  var seneca = this
  var desc
  var conn
  var db

  var ColumnNameParsing = {
    fromColumnName: opts.fromColumnName,
    toColumnName: opts.toColumnName
  }

  seneca.use(StandardQueryPlugin, ColumnNameParsing)
  var StandardQuery
  seneca.ready(function () {
    StandardQuery = seneca.export('standard-query/utils')
  })

  var internals = {}

  // function error (query, args, err/*, next*/) {
  //     if (err) {
  //       var errorDetails = {
  //         message: err.message,
  //         err: err,
  //         stack: err.stack,
  //         query: query
  //       }
  //       seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
  //       // next ({code: 'entity/error', store: name})
  //
  //       if ('ECONNREFUSED' === err.code || 'notConnected' === err.message || 'Error: no open connections' === err) {
  //         minwait = opts.minwait
  //         if (minwait) {
  //           reconnect(args)
  //         }
  //       }
  //
  //       return true
  //     }
  //
  //     return false
  //   }

  function configure (spec, cb) {
    if (!_.isUndefined(spec.connect) && !spec.connect) {
      return cb()
    }

    var conf = spec || {}

    var dbopts = seneca.util.deepextend({
      host: '127.0.0.1',
      port: 28015,
      db: 'test',
      discovery: true,
      pingInterval: 2
    }, conf)

    var rethinkOpts = {
      servers: [{host: dbopts.host, port: dbopts.port}],
      db: dbopts.db,
      discovery: true,
      pingInterval: 2
    }
    db = dbopts.db
    r = require('rethinkdbdash')(rethinkOpts)
       var applyDbTemplate = require('rethinkdb-template').default
    var template = [
  {
    db: db,
    tables: [
      {
        table: 'licences',
        indexes: [
          {
            index: 'licence_id'
          },
          { index: '_id' }
        ],
      },
      { table: 'clients' },
      { table: 'distributeurs' },
      { table: 'commandes' },
      { table: 'devices' }
    ]
  }
];

  var rethinkQuery = applyDbTemplate({template: template, r: r});
  rethinkQuery.run().then(function() {
        return cb();
  });
 }

  function tablename (canon) {
    // console.log('tablename:canon', canon)
    // var canon = entity.canon$({object: true})
    return (canon.base ? canon.base + '_' : '') + canon.name
  }

  var store = {
    name: storename,

    close: function (args, cb) {
      return cb()
    },

    save: function (args, cb) {
      var ent = args.ent
      var create = (null == ent.id)
      // var create = null | ent.id
      var canon = ent.canon$({object: true})
      var zone = canon.zone
      var base = canon.base
      var name = canon.name

      var table = tablename(canon)

      if (create) {
        if (ent.id$) {
          var id = ent.id$
          delete ent.id$
          do_save(id)
        }
        else {
          this.act(
            {role: 'basic', cmd: 'generate_id',
            name: name, base: base, zone: zone},
            function (err, id) {
              if (err) return cb(err)
              do_save(id)
            }
          )
        }
      }
      else {
        do_save(ent.id)
      }

      function do_save (id) {
        // console.log('do_save', id)
        var rdent = ent.data$(true, 'string')

        if (id) {
          rdent.id = id
        }

        rdent.entity$ = ent.entity$

        return r.table(table).get(rdent.id).run().then(function (result) {
        // r.db(db).table(table).get(rdent.id).run(conn).then(function (result) {
          if (!result)
            return do_insert(rdent)
          else
            return do_update(result, rdent)
        }).catch(err => cb)
      }

      function do_update (prev, rdent) {
        var obj = seneca.util.deepextend(prev, rdent)
        obj = seneca.util.clean(obj)
        return r.table(table).get(rdent.id).update(obj, {returnChanges: true}).run().then(function (result) {
        // r.db(db).table(table).get(rdent.id).update(obj, {returnChanges: true}).run(conn).then(function (result) {
          // console.log(result.changes)
          if (result && result.replaced > 0)
            return cb(null, ent.make$(result.changes[0].new_val))
          else
            return cb(null, ent.make$(rdent))
        }).catch(err => cb)
      }

      function do_insert (rdent) {
        rdent = seneca.util.clean(rdent)
        return r.table(table).insert(rdent, {returnChanges: true}).run().then(function (result) {
//        r.db(db).table(table).insert(rdent, {returnChanges: true}).run(conn).then(function (result) {
          return cb(null, result ? ent.make$(result.changes[0].new_val) : null)
        }).catch(err => cb)
      }
    },

    load: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var canon = qent.canon$({object: true})
      var name = canon.name
      var table = tablename(canon)
      // var query = q.id ? r.db(db).table(table).get(q.id) : r.db(db).table(table).filter(q).limit(1)
      // query.run(conn).then(function (cursor) {
      var query = q.id ? r.table(table).get(q.id) : r.table(table).filter(q).limit(1)
      query.run().then(function (results) {
        if (_.isArray(results) === false)
          results = [results]
        return cb(null, results.length ? qent.make$(results[0]) : null)
      }).catch(err => cb)
    },

    list: function (args, cb) {
      // console.log('qent', args.qent)
      var qent = args.qent
      var q = args.q

      var canon = qent.canon$({object: true})
      var table = tablename(canon)
      var name = canon.name
      // console.log('q', q)
      // console.log('canon', canon)
      // console.log('table', tablename(canon))
      r.table(table).filter(q).run().then(results => {
        // var list = []
        //  if (err) return cb(err)
        if (results)
          results = results.map(function (item) { return qent.make$(item) })
        return cb(null, results)
        // if (cursor) {
        //   cursor.eachAsync(function (row) {
        //   // console.log('row',row)
        //   // var attrs = internals.transformDBRowToJSObject(row)
        //     var attrs = internals.transformDBRowToJSObject(row)
        //   // console.log('attrs',attrs)
        //     var ent = StandardQuery.makeent(qent, attrs)
        //   // console.log(row)
        //   // console.log('ent',ent)
        //     list.push(qent.make$(attrs))
        //
        //   // list.push(qent.make$(row));
        //   }, function (final) {
        //     // the 'final' argument will only be defined when there is an error
        //     // console.log('Final called with:', list);
        //     cb(null, list)
        //     list = []
        //     return
        //   })
        // } else {
        //   cb(null, list)
        //   list = []
        //   return
        // }
      }).catch(err => cb)
    },

    remove: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var canon = qent.canon$({object: true})
      var table = tablename(canon)
      var name = canon.name

      var all = q.all$ // default false
      var load = _.isUndefined(q.load$) ? true : q.load$ // default true

      if (all) {
        // r.db(db).table(table)
        // .delete({returnChanges: load})
        // .run(conn)
        r.table(table)
        .delete({returnChanges: load})
        .run()
        .then(function (result) {
          return cb(null, _.map(load ? result.changes : [], function (e) {
            return qent.make$(e.old_val)
          }))
        })
        .catch(err => cb)
      }
      else {
        var q_clean = _.omit(q, function (k) {
          return _.endsWith(k, '$')
        }) // Remove cruft
        // r.db(db).table(table).filter(q_clean)
        // .delete({returnChanges: load})
        // .run(conn)
        r.table(table).filter(q_clean)
        .delete({returnChanges: load})
        .run()
        .then(function (result) {
          return cb(null, _.map(load ? result.changes : [], function (e) {
            return qent.make$(e.old_val)
          }))
        })
        .catch(err => cb)
      }
    },

    native: function (args, cb) {
      var ent = args.ent
      var canon = ent.canon$({object: true})
      var name = canon.name

      args.exec(null, {
        r: r,
        db: db,
        table: name,
        ent: ent
      })
    }
  }

  internals.transformDBRowToJSObject = function (row) {
    var obj = {}
    for (var attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[StandardQuery.fromColumnName(attr)] = row[attr]
      }
    }
    return obj
  }

  var meta = seneca.store.init(seneca, opts, store)
  desc = meta.desc

  seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
    configure(opts, function (err) {
      if (err) {
        return seneca.die('store', err, {
          store: store.name,
          desc: desc
        })
      }
      return done()
    })
  })

  return {
    name: store.name,
    tag: meta.tag
  }
}
