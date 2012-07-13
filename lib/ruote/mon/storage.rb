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

      collection('schedules').find(
        'at' => { '$lte' => Ruote.time_to_utc_s(now) }
      ).to_a
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

      begin

        collection(msg).insert(msg)

      rescue BSON::InvalidKeyName
        #
        # Seems like there is some kind of issue when inserting a string
        # key that begins with '$'... This is a workaround... Need to
        # send issue report to the maintainer of the mongo db ruby driver...
        #
        collection(msg).update({ '_id' => msg['_id'] }, msg, :upsert => true)
      end
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
          doc,
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
        collection(doc).find_one('_id' => doc['_id']) || true
      end
    end

    def get(type, key)

      collection(type).find_one('_id' => key)
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
        collection(doc).find_one('_id' => doc['_id']) || true
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

      return cursor.count if opts['count']

      cursor.sort(
        '_id', opts['descending'] ? Mongo::DESCENDING : Mongo::ASCENDING)

      cursor.skip(opts['skip'])
      cursor.limit(opts['limit'])

      cursor.to_a
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

    protected

    def collection(doc_or_type)

      @db.collection(
        doc_or_type.is_a?(String) ? doc_or_type : doc_or_type['type'])
    end
  end
end
end

