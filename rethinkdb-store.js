'use strict'

var _ = require('lodash')
var r = require('rethinkdb')

var StandardQueryPlugin = require('seneca-standard-query')

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

  function configure (spec, cb) {
    if (!_.isUndefined(spec.connect) && !spec.connect) {
      return cb()
    }

    var conf = spec || {}

    var dbopts = seneca.util.deepextend({
      host: '127.0.0.1',
      port: 28015,
      db: 'test'
    }, conf)
    db = dbopts.db

    r.connect(dbopts, function (err, connection) {
      if (err)
        return seneca.die('connect', err, conf)

      conn = connection
      seneca.log.debug('init', 'connect')
      cb()
    })
  }

  function tablename (canon) {
    // console.log('tablename:canon', canon)
    // var canon = entity.canon$({object: true})
    return (canon.base ? canon.base + '_' : '') + canon.name
  }

  var store = {
    name: storename,

    close: function (args, cb) {
      conn.close(function () {
        cb()
      })
    },

    save: function (args, cb) {
      var ent = args.ent

      var create = (null == ent.id)
      // var create = null | ent.id

      var canon = ent.canon$({object: true})
      var zone = canon.zone
      var base = canon.base
      var name = canon.name

      // var qent = args.qent;
      // var q = args.q;

      // var canon = qent.canon$({object: true});
      var table = tablename(canon)
      // var name = canon.name;

      // console.log('table', table)

      if (create) {
        if (ent.id$) {
          var id = ent.id$
          delete ent.id$
          do_save(id)
        }
        else {
          // console.log('Create : no id found')
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
        // console.log("do_save", id)
        var rdent = ent.data$(true, 'string')

        if (id) {
          rdent.id = id
        }

        rdent.entity$ = ent.entity$

        r.db(db).table(table).get(rdent.id).run(conn, function (err, result) {
          if (err) return cb(err)

          if (!result)
            do_insert(rdent)
          else
            do_update(result, rdent)
        })
      }

      function do_update (prev, rdent) {
        var obj = seneca.util.deepextend(prev, rdent)
        obj = seneca.util.clean(obj)
        r.db(db).table(table).get(rdent.id).update(obj, {returnChanges: true}).run(conn, function (err, result) {
          // console.log(result.changes)
          if (err)
            cb(err)
          else if (result.replaced > 0)
            cb(null, ent.make$(result.changes[0].new_val))
          else
            cb(null, ent.make$(rdent))
        })
      }

      function do_insert (rdent) {
        rdent = seneca.util.clean(rdent)
        r.db(db).table(table).insert(rdent, {returnChanges: true}).run(conn, function (err, result) {
          if (err) return cb(err)
          cb(null, ent.make$(result.changes[0].new_val))
        })
      }
    },

    load: function (args, cb) {
      // console.log('load')
      var qent = args.qent
      var q = args.q

      var canon = qent.canon$({object: true})
      var name = canon.name
      var table = tablename(canon)

      const query = q.id ? r.db(db).table(table).get(id) : r.db(db).table(table).filter(q).limit(1)
      query.run(conn, function (err, cursor) {
        if (err) return cb(err)
        if (cursor.toArray) {
          cursor.toArray((err, results) => {
            if (err) return cb(err)
            cb(null, results.length ? qent.make$(results[0]) : null)
          })
        } else {
          cb(null, cursor ? qent.make$(cursor) : null)
        }
      })
    },

    list: function (args, cb) {
      // console.log('qent',args.qent)
      var qent = args.qent
      var q = args.q

      var canon = qent.canon$({object: true})
      var table = tablename(canon)
      var name = canon.name
      var list = []
      // console.log('canon',canon)
      // console.log('table', tablename(canon))

      r.db(db).table(table).filter(q).run(conn, function (err, cursor) {
        if (err) return cb(err)

        cursor.eachAsync(function (row) {
          // console.log('row',row)
          // var attrs = internals.transformDBRowToJSObject(row)
          var attrs = internals.transformDBRowToJSObject(row)
          // console.log('attrs',attrs)
          var ent = StandardQuery.makeent(qent, attrs)
          // console.log(row)
          // console.log('ent',ent)
          list.push(qent.make$(attrs))

          // list.push(qent.make$(row));
        }, function (final) {
            // the 'final' argument will only be defined when there is an error
            // console.log('Final called with:', list);
          cb(null, list)
        })
      })
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
        r.db(db).table(table).delete({returnChanges: load}).run(conn, function (err, result) {
          if (err) return cb(err)

          cb(null, _.map(load ? result.changes : [], function (e) {
            return qent.make$(e.old_val)
          }))
        })
      }
      else {
        var q_clean = _.omit(q, function (k) {
          return _.endsWith(k, '$')
        }) // Remove cruft

        r.db(db).table(table).filter(q_clean).delete({returnChanges: load}).run(conn, function (err, result) {
          if (err) return cb(err)

          cb(null, _.map(load ? result.changes : [], function (e) {
            return qent.make$(e.old_val)
          }))
        })
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
