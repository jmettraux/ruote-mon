# encoding: UTF-8

#--
# Copyright (c) 2011-2012, John Mettraux, jmettraux@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files(the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Made in Japan.
#++

require 'mongo'

require 'ruote/storage/base'
require 'ruote/mon/version'


module Ruote
module Mon

  class Storage

    include Ruote::StorageBase

    TYPES = %w[
      msgs schedules expressions workitems errors
      configurations variables trackers history
    ]

    attr_reader :db

    def initialize(mongo_db, options={})

      @db = mongo_db
      @options = options

      #collection('msgs').drop_index('_id_')
        # can't do that...

      (TYPES - %w[ msgs schedules ]).each do |t|
        collection(t).ensure_index('_wfid')
        collection(t).ensure_index([ [ '_id', 1 ], [ '_rev', 1 ] ])
      end
      collection('schedules').ensure_index('_wfid')
      collection('schedules').ensure_index('at')

      replace_engine_configuration(options)
    end

    def get_schedules(delta, now)

      from_mongo(collection('schedules').find(
        'at' => { '$lte' => Ruote.time_to_utc_s(now) }
      ).to_a)
    end

    # Returns true if the doc was successfully deleted.
    #
    def reserve(doc)

      r = collection(doc).remove(
        { '_id' => doc['_id'] },
        :safe => true)

      r['n'] == 1
    end

    # Puts a msg. Doesn't use :safe => true, it's always an insert with a
    # new id.
    #
    def put_msg(action, options)

      msg = prepare_msg_doc(action, options)
      msg['put_at'] = Ruote.now_to_utc_s

      msg['_rev'] = 0
        # in case of msg replay

      collection(msg).insert(to_mongo(msg))
    end

    def put(doc, opts={})

      original = doc
      doc = doc.dup

      doc['_rev'] = (doc['_rev'] || -1) + 1
      doc['_wfid'] = doc['_id'].split('!').last
      doc['put_at'] = Ruote.now_to_utc_s

      if doc['type'] == 'schedules'
        doc['_wfid'] = doc['_wfid'].split('-')[0..-2].join('-')
      end

      r = begin
        collection(doc).update(
          { '_id' => doc['_id'], '_rev' => original['_rev'] },
          to_mongo(opts[:update_rev] ? Ruote.fulldup(doc) : doc),
          :safe => true, :upsert => original['_rev'].nil?)
      rescue Mongo::OperationFailure
        false
      end

      if r && (r['updatedExisting'] || original['_rev'].nil?)
        original.merge!(
          '_rev' => doc['_rev'], 'put_at' => doc['put_at']
        ) if opts[:update_rev]
        nil
      else
        from_mongo(collection(doc).find_one('_id' => doc['_id']) || true)
      end
    end

    def get(type, key)

      from_mongo(collection(type).find_one('_id' => key))
    end

    def delete(doc)

      rev = doc['_rev']

      raise ArgumentError.new("can't delete doc without _rev") unless rev

      r = collection(doc).remove(
        { '_id' => doc['_id'], '_rev' => doc['_rev'] },
        :safe => true)

      if r['n'] == 1
        nil
      else
        from_mongo(collection(doc).find_one('_id' => doc['_id']) || true)
      end
    end

    def get_many(type, key=nil, opts={})

      opts = Ruote.keys_to_s(opts)
      keys = key ? Array(key) : nil

      cursor = if keys.nil?
        collection(type).find
      elsif keys.first.is_a?(Regexp)
        collection(type).find('_id' => { '$in' => keys })
      else # a String
        collection(type).find('_wfid' => { '$in' => keys })
      end

      paginate(cursor, opts)
    end

    def ids(type)

      collection(type).find(
        {},
        :fields => [], :sort => [ '_id', Mongo::ASCENDING ]
      ).collect { |d|
        d['_id']
      }
    end

    def purge!

      TYPES.each { |t| collection(t).remove }
    end

    # Shuts this storage down.
    #
    def close

      @db.connection.close
    end

    # Shuts this storage down.
    #
    def shutdown

      @db.connection.close
    end

    # Mainly used by ruote's test/unit/ut_17_storage.rb
    #
    def add_type(type)

      # nothing to be done
    end

    # Nukes a db type and reputs it(losing all the documents that were in it).
    #
    def purge_type!(type)

      collection(type).remove
    end

    #--
    # workitem methods
    #++

    # Note: no check on value, MongoDB specific queries can be used...
    #
    # http://www.mongodb.org/display/DOCS/Advanced+Queries
    #
    def by_field(type, key, value)

      docs = paginate(
        collection(type).find("fields.#{key}" => value),
        {})

      docs.is_a?(Array) ? docs.collect { |h| Ruote::Workitem.new(h) } : docs
    end

    def by_participant(type, participant_name, opts={})

      docs = paginate(
        collection(type).find('participant_name' => participant_name),
        opts)

      docs.is_a?(Array) ? docs.collect { |h| Ruote::Workitem.new(h) } : docs
    end

    def query_workitems(query)

      docs = paginate(
        collection('workitems').find(
          query.each_with_object({}) { |(k, v), h| h["fields.#{k}"] = v }),
        {})

      docs.is_a?(Array) ? docs.collect { |h| Ruote::Workitem.new(h) } : docs
    end

    protected

    # Given a doc, returns the MongoDB collection it should go to.
    #
    def collection(doc_or_type)

      @db.collection(
        doc_or_type.is_a?(String) ? doc_or_type : doc_or_type['type'])
    end

    # Given a cursor, applies the count/skip/limit/descending options
    # if requested.
    #
    def paginate(cursor, opts)

      return cursor.count if opts['count']

      cursor.sort(
        '_id', opts['descending'] ? Mongo::DESCENDING : Mongo::ASCENDING)

      cursor.skip(opts['skip'])
      cursor.limit(opts['limit'])

      from_mongo(cursor.to_a)
    end

    # Prepares the doc for insertion in MongoDB (takes care of keys beginning
    # with '$' and/or containing '.')
    #
    def to_mongo(doc)

      # vertical tilde and ogonek to the rescue

      Ruote.deep_mutate(doc, [ /^\$/, /\./ ]) { |h, k, v|
        h.delete(k)
        h[k.gsub(/^\$/, 'ⸯ$').gsub(/\./, '˛')] = v
      }
    end

    # The real work being #from_mongo is done here.
    #
    def _from_mongo(doc)

      # vertical tilde and ogonek to the rescue

      Ruote.deep_mutate(doc, [ /^ⸯ\$/, /˛/ ]) { |h, k, v|
        h.delete(k)
        h[k.gsub(/^ⸯ\$/, '$').gsub(/˛/, '.')] = v
      }
    end

    # Prepare the doc for consumption out of MongoDB (takes care of keys
    # beginning with '$' and/or containing '.')
    #
    def from_mongo(docs)

      case docs
        when true, nil then docs
        when Array then docs.collect { |doc| _from_mongo(doc) }
        else _from_mongo(docs)
      end
    end
  end
end
end

