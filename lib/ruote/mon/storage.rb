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

    def initialize(moped_session, options={})

      @session = moped_session
      @options = options

      #collection('msgs').drop_index('_id_')
        # can't do that...

      (TYPES - %w[ msgs schedules ]).each do |t|
        collection(t).indexes.create({ '_wfid' => 1 })
        collection(t).indexes.create({ '_id' => 1, '_rev' => 1 })
      end
      collection('schedules').indexes.create({ '_wfid' => 1 })
      collection('schedules').indexes.create({ 'at' => 1 })

      replace_engine_configuration(options)
    end

    def get_schedules(delta, now)

      from_mongo(collection('schedules').find(
        { '$lte' => Ruote.time_to_utc_s(now) }
      ).to_a)
    end

    # Returns true if the doc was successfully deleted.
    #
    def reserve(doc)

      @session.with(safe: true) do |session|
        session[doc['type']].find(
          { '_id' => doc['_id'] }
        ).remove_all
      end

      true
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
        @session.with(safe: true) do |session|
          d =  session[doc['type']].find({ '_id' => doc['_id'], '_rev' => original['_rev'] })
          if original['_rev'].nil?
            d.upsert(to_mongo(opts[:update_rev] ? Ruote.fulldup(doc) : doc))
          else
            d.update(to_mongo(opts[:update_rev] ? Ruote.fulldup(doc) : doc))
          end
        end
      rescue
        false
      end

      if r && (r['updatedExisting'] || original['_rev'].nil?)
        original.merge!(
          '_rev' => doc['_rev'], 'put_at' => doc['put_at']
        ) if opts[:update_rev]
        nil
      else
        from_mongo(collection(doc).find('_id' => doc['_id']).one || true)
      end
    end

    def get(type, key)

      from_mongo(collection(type).find({ '_id' => key }).one)
    end

    def delete(doc)

      rev = doc['_rev']

      raise ArgumentError.new("can't delete doc without _rev") unless rev

      @session.with(safe: true) do |session|
        r = session[doc['type']].remove(
          { '_id' => doc['_id'], '_rev' => doc['_rev'] }
        )
      end

      if r['n'] == 1
        nil
      else
        from_mongo(collection(doc).find('_id' => doc['_id']).one || true)
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

      collection(type).find.sort(
        { '_id' => 1 }
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

      @session.disconnect
    end

    # Shuts this storage down.
    #
    def shutdown

      @session.disconnect
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
    def by_field(type, key, value, opts={})

      value = { '$exists' => true } if value.nil?

      paginate_workitems(
        collection(type).find("fields.#{key}" => value),
        opts)
    end

    def by_participant(type, participant_name, opts={})

      paginate_workitems(
        collection(type).find('participant_name' => participant_name),
        opts
     )
    end

    def query_workitems(query)

      query = Ruote.keys_to_s(query)

      opts = {}
      opts['count'] = query.delete('count')
      opts['skip'] = query.delete('skip') || query.delete('offset')
      opts['limit'] = query.delete('limit')
      opts['descending'] = query.delete('descending')

      wfid = query.delete('wfid')
      pname = query.delete('participant') || query.delete('participant_name')

      query = query.each_with_object({}) { |(k, v), h| h["fields.#{k}"] = v }

      query['wfid'] = wfid if wfid
      query['participant_name'] = pname if pname

      paginate_workitems(
        collection('workitems').find(query),
        opts
      )
    end

    protected

    # Given a doc, returns the MongoDB collection it should go to.
    #
    def collection(doc_or_type)

      collection = doc_or_type.is_a?(String) ? doc_or_type : doc_or_type['type']
      @session[collection]
    end

    # Given a cursor, applies the count/skip/limit/descending options
    # if requested.
    #
    def paginate(cursor, opts)

      opts = Ruote.keys_to_s(opts)

      return cursor.count if opts['count']

      cursor.sort(
        { '_id' => opts['descending'] ? -1 : 1 }
      )

      cursor.skip(opts['skip'])
      cursor.limit(opts['limit'])

      from_mongo(cursor.to_a)
    end

    # Wrapping around #paginate for workitems.
    #
    def paginate_workitems(cursor, opts)

      docs = paginate(cursor, opts)

      docs.is_a?(Array) ? docs.collect { |h| Ruote::Workitem.new(h) } : docs
    end

    # Prepares the doc for insertion in MongoDB (takes care of keys beginning
    # with '$' and/or containing '.')
    #
    def to_mongo(doc)

      # vertical tilde and ogonek to the rescue

      # rekey(doc) { |k| k.to_s.gsub(/^\$/, 'ⸯ$').gsub(/\./, '˛') }
      doc
    end

    # Prepare the doc for consumption out of MongoDB (takes care of keys
    # beginning with '$' and/or containing '.')
    #
    def from_mongo(docs)

      # rekey(docs) { |k| k.gsub(/^ⸯ\$/, '$').gsub(/˛/, '.') }
      docs
    end

    # rekeys hashes and sub-hashes. Simpler than Ruote.deep_mutate
    #
    def rekey(o, &block)

      case o
        when Hash; o.remap { |(k, v), h| h[block.call(k)] = rekey(v, &block) }
        when Array; o.collect { |e| rekey(e, &block) }
        when Symbol; o.to_s
        else o
      end
    end
  end
end
end

