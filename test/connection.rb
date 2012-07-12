
#
# testing ruote-mon
#
# Tue Nov 22 16:54:57 JST 2011
#

require 'ruote-mon'


def new_storage(opts)

  #con = Mongo::Connection.new
    #
    # not thread-safe

  #con = Mongo::Connection.new(nil, nil, :refresh_mode => :sync)
    #
    # http://groups.google.com/group/mongodb-user/browse_thread/thread/7d09df9fa765891e
    #
    # but it doesn't work.

  con = Mongo::Connection.new(
    '127.0.0.1', 27017, :pool_size => 14, :pool_timeout => 3)

  Ruote::Mon::Storage.new(con['ruote_mon_test'], opts)
end

